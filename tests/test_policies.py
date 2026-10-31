"""Grants, ownership, admission limits, disk space and retention (no HTTP)."""

import errno
import os
import time

import pytest

from xferkit import (
    GrantSigner,
    InsufficientStorage,
    InvalidGrant,
    LocalStorage,
    PayloadTooLarge,
    QuotaExceeded,
    RateLimiter,
    TooManyUploads,
    UploadManager,
    UploadNotFound,
)
from xferkit.protocol import KiB, MiB

SECRET = "s" * 32


def make_manager(tmp_path, **kwargs):
    kwargs.setdefault("min_chunk_size", 1 * KiB)
    kwargs.setdefault("default_chunk_size", 4 * KiB)
    storage = kwargs.pop("storage", None) or LocalStorage(tmp_path / "store", min_free_bytes=0)
    return UploadManager(storage, **kwargs)


# ---------------------------------------------------------------------------- grants


def test_grant_round_trip_and_scope():
    now = [1_000_000.0]
    signer = GrantSigner(SECRET, clock=lambda: now[0])
    token = signer.issue(owner="alice", prefix="/uploads", methods="*", ttl=60, max_size=10)
    grant = signer.verify(token, method="PUT", path="/uploads/abc/parts/3")
    assert grant.owner == "alice" and grant.max_size == 10

    assert signer.verify(token, method="POST", path="/uploads").owner == "alice"
    with pytest.raises(InvalidGrant):  # prefix is segment-aware
        signer.verify(token, method="GET", path="/uploads2/x")
    with pytest.raises(InvalidGrant):
        signer.verify(token, method="GET", path="/files/x")

    read_only = signer.issue(prefix="/files/a.tif")
    assert signer.verify(read_only, method="HEAD", path="/files/a.tif").owner is None
    with pytest.raises(InvalidGrant):
        signer.verify(read_only, method="PUT", path="/files/a.tif")

    now[0] += 61
    with pytest.raises(InvalidGrant, match="expired"):
        signer.verify(token)


def test_grant_tampering_and_rotation():
    old = GrantSigner(SECRET)
    token = old.issue(owner="alice")
    version, body, mac = token.split(".")
    forged_body = body[:-2] + ("AA" if body[-2:] != "AA" else "BB")
    for bad in (f"{version}.{forged_body}.{mac}", f"v2.{body}.{mac}", "garbage", ""):
        with pytest.raises(InvalidGrant):
            old.verify(bad)
    with pytest.raises(InvalidGrant):
        GrantSigner("t" * 32).verify(token)
    rotated = GrantSigner("t" * 32, previous_secrets=[SECRET])
    assert rotated.verify(token).owner == "alice"  # old grants still work
    with pytest.raises(ValueError):
        GrantSigner("short")


def test_sign_url_keeps_query_and_scopes_to_path():
    signer = GrantSigner(SECRET)
    url = signer.sign_url("https://h.example/uploads/abc/content?x=1&xk=old", ttl=60)
    assert url.startswith("https://h.example/uploads/abc/content?x=1&xk=")
    token = url.split("xk=")[1]
    assert signer.verify(token, method="GET", path="/uploads/abc/content")
    with pytest.raises(InvalidGrant):
        signer.verify(token, method="GET", path="/uploads/other/content")


# ---------------------------------------------------------------------------- ownership


def test_owner_is_recorded_and_enforced(tmp_path):
    manager = make_manager(tmp_path)
    mine = manager.create_upload("a", 5, owner="alice")
    anon = manager.create_upload("b", 5)
    manager.ensure_access(mine.upload_id, "alice")
    for stranger in ("bob", None):
        with pytest.raises(UploadNotFound):
            manager.ensure_access(mine.upload_id, stranger)
    manager.ensure_access(anon.upload_id, "bob")  # anonymous uploads: id is the capability
    manager.put_part(mine.upload_id, 0, b"12345")
    completed = manager.complete(mine.upload_id)
    assert completed.owner == "alice"
    assert manager.owner_of(mine.upload_id) == "alice"  # still known after completion
    with pytest.raises(UploadNotFound):
        manager.ensure_access(mine.upload_id, "bob")


def test_client_tokens_are_scoped_per_owner(tmp_path):
    manager = make_manager(tmp_path)
    a, created_a = manager.open_upload("f", 5, client_token="same_token_01", owner="alice")
    b, created_b = manager.open_upload("f", 5, client_token="same_token_01", owner="bob")
    assert created_a and created_b and a["upload_id"] != b["upload_id"]
    again, created = manager.open_upload("f", 5, client_token="same_token_01", owner="alice")
    assert not created and again["upload_id"] == a["upload_id"]


# ---------------------------------------------------------------------------- admission


def test_pending_limits(tmp_path):
    manager = make_manager(
        tmp_path, max_pending_per_owner=2, max_pending_bytes_per_owner=100, max_pending_uploads=3
    )
    manager.create_upload("a", 40, owner="alice")
    manager.create_upload("b", 40, owner="alice")
    with pytest.raises(TooManyUploads) as info:
        manager.create_upload("c", 1, owner="alice")
    assert info.value.status_code == 429 and info.value.details["retry_after"] > 0
    with pytest.raises(PayloadTooLarge):
        manager.create_upload("big", 101, owner="bob")
    manager.create_upload("d", 90, owner="bob")
    with pytest.raises(TooManyUploads):  # server-wide limit (3 pending)
        manager.create_upload("e", 1, owner="carol")


def test_pending_bytes_limit_counts_in_flight_bytes(tmp_path):
    manager = make_manager(tmp_path, max_pending_bytes_per_owner=100)
    first = manager.create_upload("a", 80, owner="alice")
    with pytest.raises(TooManyUploads):
        manager.create_upload("b", 30, owner="alice")
    manager.abort(first.upload_id)
    manager.create_upload("b", 30, owner="alice")


def test_before_create_hook(tmp_path):
    seen = []

    def policy(request):
        seen.append(request)
        if not request.filename.lower().endswith((".tif", ".jpg")):
            raise QuotaExceeded("only imagery is accepted here")

    manager = make_manager(tmp_path, before_create=policy)
    manager.create_upload("scene.TIF", 10, owner="alice", metadata={"k": "v"})
    with pytest.raises(QuotaExceeded):
        manager.create_upload("notes.txt", 10, owner="alice")
    assert seen[0].owner == "alice" and seen[0].size == 10 and seen[0].metadata == {"k": "v"}


def test_rate_limiter():
    now = [0.0]
    limiter = RateLimiter(rate=2, burst=3, clock=lambda: now[0])
    assert [limiter.acquire("a") for _ in range(3)] == [0, 0, 0]
    assert limiter.acquire("a") == pytest.approx(0.5)
    assert limiter.acquire("b") == 0  # independent keys
    now[0] += 0.5
    assert limiter.acquire("a") == 0


# ---------------------------------------------------------------------------- disk space


def test_space_check_counts_unfinished_uploads(tmp_path):
    storage = LocalStorage(tmp_path / "store", min_free_bytes=0)
    manager = make_manager(tmp_path, storage=storage, max_upload_size=None)
    free = __import__("shutil").disk_usage(storage.root).free
    big = manager.create_upload("big", free // 2, chunk_size=256 * MiB)
    assert storage.outstanding_bytes() >= free // 2 - 256 * MiB  # sparse: nothing written yet
    with pytest.raises(InsufficientStorage) as info:
        manager.create_upload("big2", free // 2 + 10 * MiB, chunk_size=256 * MiB)
    assert info.value.status_code == 507
    manager.abort(big.upload_id)
    assert storage.outstanding_bytes() == 0


def test_min_free_bytes_reserve(tmp_path):
    free = __import__("shutil").disk_usage(tmp_path).free
    manager = make_manager(tmp_path, storage=LocalStorage(tmp_path / "s", min_free_bytes=free))
    with pytest.raises(InsufficientStorage):
        manager.create_upload("a", 1)
    unchecked = make_manager(tmp_path, storage=LocalStorage(tmp_path / "t", min_free_bytes=None))
    unchecked.create_upload("a", 1)


def test_disk_full_during_part_is_507(tmp_path, monkeypatch):
    manager = make_manager(tmp_path)
    session = manager.create_upload("a", 4096)
    sink = manager.open_part(session.upload_id, 0)

    def full(data):
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(sink._fh, "write", full)
    with pytest.raises(InsufficientStorage):
        sink.write(b"x" * 10)
    sink.abort()
    manager.put_part(session.upload_id, 0, b"x" * 4096)  # the part lock was released


@pytest.mark.skipif(not hasattr(os, "posix_fallocate"), reason="Linux only")
def test_preallocate_reserves_blocks(tmp_path):
    storage = LocalStorage(tmp_path / "store", min_free_bytes=0, preallocate=True)
    manager = make_manager(tmp_path, storage=storage)
    session = manager.create_upload("a", 8 * MiB, chunk_size=1 * MiB)
    data = storage.root / "uploads" / session.upload_id / "data"
    assert data.stat().st_blocks * 512 >= 8 * MiB
    assert storage.outstanding_bytes() == 0


# ---------------------------------------------------------------------------- retention


def test_completed_records_expire(tmp_path):
    now = [time.time()]  # completion times are stamped with the real clock
    manager = make_manager(tmp_path, completed_ttl=100, clock=lambda: now[0])
    keep_file = make_manager(tmp_path, completed_ttl=100, clock=lambda: now[0])
    s = manager.create_upload("a", 5)
    manager.put_part(s.upload_id, 0, b"12345")
    completed = manager.complete(s.upload_id)
    now[0] += 50
    assert manager.expire_completed() == []
    now[0] += 51
    assert keep_file.cleanup_expired() == []  # pending sessions: none
    with pytest.raises(UploadNotFound):
        manager.get_completed(s.upload_id)
    assert completed.path.exists()  # remove_files_on_expiry is off by default

    deleting = make_manager(
        tmp_path, completed_ttl=100, remove_files_on_expiry=True, clock=lambda: now[0]
    )
    s2 = deleting.create_upload("b", 5)
    deleting.put_part(s2.upload_id, 0, b"12345")
    path = deleting.complete(s2.upload_id).path
    now[0] += 101
    assert deleting.expire_completed() == [s2.upload_id]
    assert not path.exists()
