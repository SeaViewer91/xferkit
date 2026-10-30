import hashlib
import os

import pytest

from xferkit import (
    ChecksumMismatch,
    InvalidPart,
    InvalidRequest,
    LocalStorage,
    PayloadTooLarge,
    UploadConflict,
    UploadExpired,
    UploadIncomplete,
    UploadManager,
    UploadNotFound,
)
from xferkit.protocol import KiB, MiB, composite_checksum, format_checksum
from xferkit.server import safe_filename


def make_manager(tmp_path, **kwargs):
    kwargs.setdefault("min_chunk_size", 1 * KiB)
    kwargs.setdefault("default_chunk_size", 4 * KiB)
    return UploadManager(tmp_path / "store", **kwargs)


def put_all(manager, session, data, order=None):
    for index in order if order is not None else range(session.total_parts):
        offset = session.part_offset(index)
        manager.put_part(session.upload_id, index, data[offset : offset + session.part_size(index)])


def test_out_of_order_upload_completes(tmp_path):
    manager = make_manager(tmp_path)
    data = os.urandom(10_000)
    session = manager.create_upload("데이터 파일.bin", len(data), metadata={"site": "부산"})
    assert (session.total_parts, session.chunk_size) == (3, 4 * KiB)

    put_all(manager, session, data, order=[2, 0])
    status = manager.get_status(session.upload_id)
    assert status.received_parts == (0, 2)
    assert status.missing_parts == [1]
    assert status.received_bytes == 4096 + (10_000 - 8192)

    put_all(manager, session, data, order=[1])
    completed, created = manager.complete_upload(session.upload_id)
    assert created
    assert completed.path.read_bytes() == data
    assert completed.path.name == "데이터 파일.bin"
    assert completed.metadata == {"site": "부산"}
    parts = [data[i : i + 4096] for i in range(0, len(data), 4096)]
    assert completed.composite_checksum == composite_checksum(
        hashlib.sha256(p).digest() for p in parts
    )
    # The session directory is gone, the record remains and completion is idempotent.
    with pytest.raises(UploadNotFound):
        manager.get_status(session.upload_id)
    again, created = manager.complete_upload(session.upload_id)
    assert not created and again.path == completed.path
    assert manager.get_completed(session.upload_id).size == len(data)


def test_on_complete_hook_runs_once(tmp_path):
    seen = []
    manager = make_manager(tmp_path, on_complete=seen.append)
    session = manager.create_upload("a.txt", 5)
    manager.put_part(session.upload_id, 0, b"hello")
    manager.complete(session.upload_id)
    manager.complete(session.upload_id)
    assert [c.upload_id for c in seen] == [session.upload_id]


def test_empty_file(tmp_path):
    manager = make_manager(tmp_path)
    session = manager.create_upload("empty", 0)
    assert session.total_parts == 0
    completed = manager.complete(session.upload_id)
    assert completed.path.read_bytes() == b""


def test_part_validation(tmp_path):
    manager = make_manager(tmp_path)
    session = manager.create_upload("f", 5000)
    uid = session.upload_id
    with pytest.raises(InvalidPart):
        manager.put_part(uid, 2, b"x")  # out of range
    with pytest.raises(InvalidPart):
        manager.put_part(uid, 0, b"x" * 10)  # too short
    with pytest.raises(PayloadTooLarge):
        manager.put_part(uid, 1, b"x" * 2000)  # last part is 904 bytes
    with pytest.raises(ChecksumMismatch):
        manager.put_part(uid, 1, b"x" * 904, checksum=format_checksum("0" * 64))
    with pytest.raises(InvalidRequest):
        manager.put_part(uid, 1, b"x" * 904, checksum="md5=123")
    assert manager.get_status(uid).received_parts == ()

    good = b"y" * 904
    record = manager.put_part(
        uid, 1, good, checksum=format_checksum(hashlib.sha256(good).hexdigest())
    )
    assert record.size == 904
    assert manager.get_status(uid).received_parts == (1,)


def test_reupload_replaces_part_and_failed_reupload_invalidates_it(tmp_path):
    manager = make_manager(tmp_path)
    session = manager.create_upload("f", 4096)
    uid = session.upload_id
    manager.put_part(uid, 0, b"a" * 4096)
    with pytest.raises(InvalidPart):
        manager.put_part(uid, 0, b"b" * 10)
    assert manager.get_status(uid).missing_parts == [0]
    manager.put_part(uid, 0, b"c" * 4096)
    assert manager.complete(uid).path.read_bytes() == b"c" * 4096


def test_concurrent_writes_to_same_part_conflict(tmp_path):
    manager = make_manager(tmp_path)
    session = manager.create_upload("f", 4096)
    with manager.open_part(session.upload_id, 0) as sink:
        sink.write(b"a" * 100)
        with pytest.raises(UploadConflict):
            manager.open_part(session.upload_id, 0)
        with pytest.raises(UploadConflict):
            manager.complete(session.upload_id)
    # Leaving the block without commit aborted the part and released the lock.
    manager.put_part(session.upload_id, 0, b"a" * 4096)
    manager.complete(session.upload_id)


def test_complete_with_missing_parts(tmp_path):
    manager = make_manager(tmp_path)
    session = manager.create_upload("f", 9000)
    manager.put_part(session.upload_id, 1, b"x" * 4096)
    with pytest.raises(UploadIncomplete) as info:
        manager.complete(session.upload_id)
    assert info.value.details["missing_parts"] == [0, 2]
    # The session is still usable afterwards.
    assert manager.get_status(session.upload_id).state == "pending"


def test_whole_file_checksum(tmp_path):
    manager = make_manager(tmp_path)
    data = os.urandom(5000)
    good = format_checksum(hashlib.sha256(data).hexdigest())
    session = manager.create_upload("f", len(data), checksum=good.upper().replace("SHA", "sha"))
    put_all(manager, session, data)
    assert manager.complete(session.upload_id).checksum == good

    bad = manager.create_upload("g", len(data), checksum=format_checksum("1" * 64))
    put_all(manager, bad, data)
    with pytest.raises(ChecksumMismatch):
        manager.complete(bad.upload_id)

    with pytest.raises(InvalidRequest):
        manager.create_upload("h", 1, checksum="sha1=abc")


def test_abort(tmp_path):
    manager = make_manager(tmp_path)
    session = manager.create_upload("f", 10)
    manager.abort(session.upload_id)
    with pytest.raises(UploadNotFound):
        manager.get_status(session.upload_id)
    with pytest.raises(UploadNotFound):
        manager.abort(session.upload_id)


def test_expiry_and_cleanup(tmp_path):
    now = [1000.0]
    manager = make_manager(tmp_path, ttl=60, clock=lambda: now[0])
    old = manager.create_upload("old", 10)
    now[0] += 30
    fresh = manager.create_upload("fresh", 10)
    now[0] += 31
    with pytest.raises(UploadExpired):
        manager.get_status(old.upload_id)
    with pytest.raises(UploadExpired):
        manager.complete(old.upload_id)
    assert manager.cleanup_expired() == [old.upload_id]
    assert manager.get_status(fresh.upload_id).session.filename == "fresh"


@pytest.mark.parametrize("bad_id", ["../../etc", "short", "a" * 200, "abc/def123", ""])
def test_malformed_ids_are_not_found(tmp_path, bad_id):
    manager = make_manager(tmp_path)
    with pytest.raises(UploadNotFound):
        manager.get_status(bad_id)
    with pytest.raises(UploadNotFound):
        manager.complete(bad_id)


def test_limits_and_chunk_size_selection(tmp_path):
    manager = make_manager(tmp_path, max_upload_size=10 * MiB, max_parts=4, max_chunk_size=8 * MiB)
    with pytest.raises(PayloadTooLarge):
        manager.create_upload("big", 11 * MiB)
    auto = manager.create_upload("auto", 10 * MiB)
    assert auto.chunk_size == 3 * MiB and auto.total_parts == 4  # grown to stay <= max_parts
    with pytest.raises(InvalidRequest):
        manager.create_upload("tiny", 10 * MiB, chunk_size=1 * KiB)  # too many parts
    with pytest.raises(InvalidRequest):
        manager.create_upload("x", 10, chunk_size=10)  # below min_chunk_size
    with pytest.raises(InvalidRequest):
        manager.create_upload("", 10)
    with pytest.raises(InvalidRequest):
        manager.create_upload("x", -1)
    with pytest.raises(InvalidRequest):
        manager.create_upload("x", 1, metadata={"k": 1})


def test_storage_survives_restart(tmp_path):
    manager = make_manager(tmp_path)
    session = manager.create_upload("f", 6000)
    manager.put_part(session.upload_id, 0, b"a" * 4096)
    reopened = UploadManager(
        LocalStorage(tmp_path / "store"), min_chunk_size=1 * KiB, default_chunk_size=4 * KiB
    )
    assert reopened.get_status(session.upload_id).received_parts == (0,)


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("../../etc/passwd", "passwd"),
        ("C:\\Users\\x\\보고서.hwp", "보고서.hwp"),
        ("..", "file"),
        ("a<b>c?.txt", "a_b_c_.txt"),
        ("CON.txt", "_CON.txt"),
        ("\u1112\u1161\u11ab.txt", "한.txt"),  # NFD (macOS) -> NFC
    ],
)
def test_safe_filename(name, expected):
    assert safe_filename(name) == expected


def test_safe_filename_truncates_on_utf8_boundary():
    name = "가" * 100 + ".tif"
    result = safe_filename(name)
    assert result.endswith(".tif")
    assert len(result.encode("utf-8")) <= 200
