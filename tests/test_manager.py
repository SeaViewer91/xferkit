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


@pytest.mark.skipif(os.name == "nt", reason="owner liveness is not checked on Windows")
def test_lock_left_by_dead_process_is_taken_over(tmp_path):
    """A server worker killed mid-part leaves parts/<n>.lock behind; the next
    request for that part must not be blocked until the timeout expires."""
    import socket
    import subprocess
    import sys

    manager = make_manager(tmp_path)
    session = manager.create_upload("f", 4096)
    lock = tmp_path / "store" / "uploads" / session.upload_id / "parts" / "0.lock"
    host = socket.gethostname().replace(" ", "_")

    dead = subprocess.Popen([sys.executable, "-c", "pass"])
    dead.wait()
    lock.write_text(f"{host} {dead.pid} 0\n")
    manager.put_part(session.upload_id, 0, b"a" * 4096)  # taken over immediately
    assert not lock.exists()

    lock.write_text(f"{host} {os.getppid()} 0\n")  # owner still alive
    with pytest.raises(UploadConflict):
        manager.put_part(session.upload_id, 0, b"a" * 4096)
    lock.write_text(f"other-host {dead.pid} 0\n")  # cannot check: keep waiting
    with pytest.raises(UploadConflict):
        manager.put_part(session.upload_id, 0, b"a" * 4096)


def test_client_token_makes_create_idempotent(tmp_path):
    manager = make_manager(tmp_path)
    first, created = manager.open_upload("a.bin", 5000, client_token="tok_ABCDEFGH1234")
    assert created and first["state"] == "pending"
    manager.put_part(first["upload_id"], 0, b"x" * 4096)

    again, created = manager.open_upload("a.bin", 5000, client_token="tok_ABCDEFGH1234")
    assert not created
    assert again["upload_id"] == first["upload_id"]
    assert again["received_parts"] == [0]  # the client can resume right away

    with pytest.raises(UploadConflict):  # same token, different file
        manager.open_upload("b.bin", 5000, client_token="tok_ABCDEFGH1234")
    with pytest.raises(InvalidRequest):
        manager.open_upload("a.bin", 5000, client_token="bad token!")

    manager.put_part(first["upload_id"], 1, b"y" * 904)
    manager.complete(first["upload_id"])
    done, created = manager.open_upload("a.bin", 5000, client_token="tok_ABCDEFGH1234")
    assert not created and done["state"] == "completed"

    other, _ = manager.open_upload("c.bin", 10, client_token="tok_ZZZZZZZZ0000")
    manager.abort(other["upload_id"])
    fresh, created = manager.open_upload("c.bin", 10, client_token="tok_ZZZZZZZZ0000")
    assert created and fresh["upload_id"] != other["upload_id"]  # aborted: new session


def test_cleanup_prunes_dangling_tokens(tmp_path):
    now = [1000.0]
    manager = make_manager(tmp_path, ttl=60, clock=lambda: now[0])
    state, _ = manager.open_upload("a", 10, client_token="tok_prune_0001")
    kept, _ = manager.open_upload("b", 5, client_token="tok_prune_0002")
    manager.put_part(kept["upload_id"], 0, b"12345")
    manager.complete(kept["upload_id"])
    now[0] += 120
    assert manager.cleanup_expired() == [state["upload_id"]]
    tokens = sorted(p.name for p in (tmp_path / "store" / "tokens").iterdir())
    assert tokens == ["tok_prune_0002"]  # completed upload keeps its token


def test_token_is_bound_before_the_session_exists(tmp_path, monkeypatch):
    """If the server dies after binding the token but before creating the
    session, the retry must create the session (no orphan, no error)."""
    manager = make_manager(tmp_path)
    storage = manager.storage

    def crash(session):
        raise SystemExit("server killed")

    monkeypatch.setattr(storage, "create", crash)
    with pytest.raises(SystemExit):
        manager.open_upload("a.bin", 10, client_token="tok_crash_00001")
    monkeypatch.undo()
    assert list((tmp_path / "store" / "uploads").iterdir()) == []

    state, created = manager.open_upload("a.bin", 10, client_token="tok_crash_00001")
    assert created and state["state"] == "pending"
    again, created = manager.open_upload("a.bin", 10, client_token="tok_crash_00001")
    assert not created and again["upload_id"] == state["upload_id"]


def test_half_created_sessions_are_invisible_and_collected(tmp_path):
    manager = make_manager(tmp_path)
    uploads = tmp_path / "store" / "uploads"
    staging = uploads / ".creating-abcdefgh1234"  # crash during create
    staging.mkdir()
    legacy = uploads / "abcdefgh5678"  # session dir without session.json
    legacy.mkdir()
    assert list(manager.storage.iter_sessions()) == []
    assert manager.storage.collect_garbage(older_than=3600) == 0  # too recent
    old = 1_000_000_000
    for path in (staging, legacy):
        os.utime(path, (old, old))
    assert manager.storage.collect_garbage(older_than=3600) == 2
    assert list(uploads.iterdir()) == []


def test_session_removal_is_crash_safe(tmp_path):
    """Completion and abort rename the session directory away before deleting
    it, so a crash mid-removal leaves only a .trash-* directory that is
    invisible to clients and collected later."""
    manager = make_manager(tmp_path)
    uploads = tmp_path / "store" / "uploads"
    session = manager.create_upload("f", 5)
    manager.put_part(session.upload_id, 0, b"12345")
    manager.complete(session.upload_id)
    assert list(uploads.iterdir()) == []

    leftover = uploads / f".trash-{session.upload_id}-deadbeef"  # crash mid-rmtree
    (leftover / "parts").mkdir(parents=True)
    assert list(manager.storage.iter_sessions()) == []
    assert manager.describe(session.upload_id)["state"] == "completed"
    manager.cleanup_expired()
    assert list(uploads.iterdir()) == []
