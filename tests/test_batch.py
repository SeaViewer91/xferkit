"""Batch session creation, auto-completion and folder uploads."""

import asyncio
import hashlib
import os
import threading

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from conftest import LiveServer
from xferkit import (
    InvalidRequest,
    LocalStorage,
    UploadManager,
    UploadNotFound,
)
from xferkit.client import (
    AsyncUploader,
    BatchUploadError,
    FailedUpload,
    RetryPolicy,
    UploadItem,
    upload_folder,
)
from xferkit.integrations.fastapi import create_upload_router
from xferkit.protocol import CHECKSUM_HEADER, KiB, format_checksum

FAST_RETRY = RetryPolicy(attempts=4, backoff=0.01, max_backoff=0.05)


def make_manager(tmp_path, **kwargs):
    kwargs.setdefault("min_chunk_size", 1 * KiB)
    kwargs.setdefault("default_chunk_size", 16 * KiB)
    return UploadManager(LocalStorage(tmp_path / "store", min_free_bytes=0), **kwargs)


def completed_records(manager):
    return list(manager.storage.iter_completed())


def make_tree(root):
    """A small folder: nested dirs, a hidden file, an empty file, a multi-part file."""
    files = {
        "a.jpg": os.urandom(3 * KiB),
        "sub/b.jpg": os.urandom(20 * KiB),
        "sub/deeper/c.txt": os.urandom(100),
        "sub/deeper/한글 이름.jpg": os.urandom(5 * KiB),
        "empty.dat": b"",
        "big.bin": os.urandom(70 * KiB),  # several 16 KiB parts
        ".hidden/secret.jpg": os.urandom(10),
        ".dotfile": b"x",
    }
    for rel, data in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    return files


# ------------------------------------------------------------------ manager level


def test_open_uploads_items_fail_independently(tmp_path):
    manager = make_manager(tmp_path, max_upload_size=10 * KiB, max_batch_size=3)
    results = manager.open_uploads(
        [
            {"filename": "a", "size": 10, "client_token": "token-aaaa"},
            {"filename": "b", "size": 20 * KiB},  # too large
            {"filename": "", "size": 1},  # invalid name
        ],
        owner="alice",
    )
    (state, created), too_large, bad_name = results
    assert created and state["owner"] == "alice"
    assert too_large.code == "payload_too_large"
    assert bad_name.code == "invalid_request"

    # Same token again: the existing upload, not a new one.
    [(again, created_again)] = manager.open_uploads(
        [{"filename": "a", "size": 10, "client_token": "token-aaaa"}], owner="alice"
    )
    assert not created_again and again["upload_id"] == state["upload_id"]

    with pytest.raises(InvalidRequest):
        manager.open_uploads([{"filename": "x", "size": 1}] * 4)


def test_describe_many_applies_ownership(tmp_path):
    manager = make_manager(tmp_path)
    mine = manager.create_upload("a", 10, owner="alice").upload_id
    theirs = manager.create_upload("b", 10, owner="bob").upload_id
    ok, hidden, unknown = manager.describe_many([mine, theirs, "nope"], caller="alice")
    assert ok["upload_id"] == mine
    assert isinstance(hidden, UploadNotFound) and isinstance(unknown, UploadNotFound)


def test_complete_if_ready_completes_once(tmp_path):
    manager = make_manager(tmp_path)
    data = os.urandom(40 * KiB)  # 3 parts
    session = manager.create_upload("x.bin", len(data), chunk_size=16 * KiB, auto_complete=True)
    plain = manager.create_upload("y.bin", 1, auto_complete=False)
    manager.put_part(plain.upload_id, 0, b"y")
    assert manager.complete_if_ready(plain.upload_id) is None  # not auto_complete

    for index in range(3):
        if index == 2:
            assert manager.complete_if_ready(session.upload_id) is None  # part missing
        chunk = data[index * 16 * KiB : (index + 1) * 16 * KiB]
        manager.put_part(session.upload_id, index, chunk)

    # Several requests see "all parts present" at once; one completes it.
    outcomes = []
    barrier = threading.Barrier(4)

    def finish():
        barrier.wait()
        outcomes.append(manager.complete_if_ready(session.upload_id))

    threads = [threading.Thread(target=finish) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    created = [o for o in outcomes if o is not None and o[1]]
    assert len(created) == 1
    assert created[0][0].path.read_bytes() == data
    # Afterwards the record is returned, never created again.
    record, created_again = manager.complete_if_ready(session.upload_id)
    assert not created_again and record.upload_id == session.upload_id


def test_auto_complete_is_persisted(tmp_path):
    manager = make_manager(tmp_path)
    session = manager.create_upload("x", 5, auto_complete=True)
    reloaded = make_manager(tmp_path).get_session(session.upload_id)
    assert reloaded.auto_complete


# ------------------------------------------------------------------ HTTP


@pytest.fixture
def batch_server(tmp_path):
    manager = make_manager(tmp_path, max_upload_size=1024 * KiB)
    completed = []

    def principal(request: Request):
        return request.headers.get("X-User")

    async def authorize(request, ctx):
        return not (ctx.action == "create" and ctx.create and ctx.create.filename == "denied.bin")

    app = FastAPI()
    app.include_router(
        create_upload_router(
            manager,
            principal=principal,
            authorize=authorize,
            on_complete=lambda record, request: completed.append(record),
        )
    )
    server = LiveServer(app)
    server.start()
    yield server, manager, completed
    server.stop()


async def test_batch_create_and_status_over_http(batch_server):
    server, _, _ = batch_server
    files = [
        {"filename": "a.bin", "size": 10, "client_token": "tok-aaaaaaaa"},
        {"filename": "denied.bin", "size": 10},
        {"filename": "huge.bin", "size": 10 * 1024 * KiB},
        {"filename": "b.bin", "size": 0, "auto_complete": True},
    ]
    async with httpx.AsyncClient(base_url=server.url, headers={"X-User": "alice"}) as http:
        r = await http.post("/uploads/batch", json={"files": files})
        assert r.status_code == 200
        a, denied, huge, b = r.json()["uploads"]
        assert a["created"] and a["owner"] == "alice" and a["state"] == "pending"
        assert denied["status"] == 403 and denied["error"]["code"] == "forbidden"
        assert huge["status"] == 413 and huge["error"]["code"] == "payload_too_large"
        assert b["created"]

        again = (await http.post("/uploads/batch", json={"files": files[:1]})).json()
        assert again["uploads"][0]["upload_id"] == a["upload_id"]
        assert again["uploads"][0]["created"] is False

        r = await http.post(
            "/uploads/batch/status", json={"upload_ids": [a["upload_id"], "missing-id"]}
        )
        ok, missing = r.json()["uploads"]
        assert ok["received_parts"] == [] and missing["status"] == 404

        # Another user sees neither the upload nor its token.
        r = await http.post(
            "/uploads/batch/status",
            json={"upload_ids": [a["upload_id"]]},
            headers={"X-User": "mallory"},
        )
        assert r.json()["uploads"][0]["status"] == 404

        too_many = {"files": [{"filename": "x", "size": 1}] * 1001}
        assert (await http.post("/uploads/batch", json=too_many)).status_code == 422


async def test_last_part_completes_auto_complete_upload(batch_server):
    server, _, completed = batch_server
    data = os.urandom(40 * KiB)
    async with httpx.AsyncClient(base_url=server.url, headers={"X-User": "alice"}) as http:
        r = await http.post(
            "/uploads",
            json={
                "filename": "auto.bin",
                "size": len(data),
                "chunk_size": 16 * KiB,
                "auto_complete": True,
            },
        )
        upload_id = r.json()["upload_id"]

        async def put(index):
            chunk = data[index * 16 * KiB : (index + 1) * 16 * KiB]
            return await http.put(
                f"/uploads/{upload_id}/parts/{index}",
                content=chunk,
                headers={CHECKSUM_HEADER: format_checksum(hashlib.sha256(chunk).hexdigest())},
            )

        first = await put(0)
        assert "completed" not in first.json()
        bodies = [r.json() for r in await asyncio.gather(put(1), put(2))]
        records = [b["completed"] for b in bodies if "completed" in b]
        assert records, bodies
        assert all(r["upload_id"] == upload_id for r in records)
        status = (await http.get(f"/uploads/{upload_id}")).json()
        assert status["state"] == "completed"
    assert len(completed) == 1  # on_complete ran exactly once
    assert completed[0].path.read_bytes() == data


# ------------------------------------------------------------------ client


@pytest.fixture
def folder_server(tmp_path):
    manager = make_manager(tmp_path)
    completed = []
    app = FastAPI()
    app.include_router(
        create_upload_router(manager, on_complete=lambda r, request: completed.append(r))
    )
    server = LiveServer(app)
    server.start()
    yield server, manager, completed
    server.stop()


async def test_upload_folder_round_trip_and_rerun(folder_server, tmp_path):
    server, manager, completed = folder_server
    root = tmp_path / "photos"
    files = make_tree(root)
    seen = []
    async with AsyncUploader(f"{server.url}/uploads", concurrency=3, retry=FAST_RETRY) as up:
        result = await up.upload_folder(
            root, metadata={"survey": "S1"}, progress=seen.append, auto_complete_below=32 * KiB
        )
    result.raise_for_failures()
    visible = {rel: data for rel, data in files.items() if not rel.startswith(".")}
    assert sorted(r.metadata["relative_path"] for r in result.uploaded) == sorted(visible)
    assert all(r.metadata["survey"] == "S1" for r in result.uploaded)
    assert not any(r.resumed for r in result.uploaded)
    by_rel = {c.metadata["relative_path"]: c for c in completed}
    for rel, data in visible.items():
        assert by_rel[rel].path.read_bytes() == data
        assert by_rel[rel].filename == rel.rsplit("/", 1)[-1]
    last = seen[-1]
    assert last.files_done == len(visible) and last.bytes_done == last.bytes_total
    assert last.bytes_total == sum(len(d) for d in visible.values())

    # Run again: nothing is uploaded twice, every file is verified.
    async with AsyncUploader(f"{server.url}/uploads", retry=FAST_RETRY) as up:
        again = await up.upload_folder(root)
    assert again.ok and all(r.resumed for r in again.uploaded)
    assert {r.upload_id for r in again.uploaded} == {r.upload_id for r in result.uploaded}
    assert len(completed_records(manager)) == len(visible)

    # A changed file is a new upload; the others are not touched.
    (root / "a.jpg").write_bytes(b"changed")
    os.utime(root / "a.jpg", ns=(1, 1))
    async with AsyncUploader(f"{server.url}/uploads", retry=FAST_RETRY) as up:
        third = await up.upload_folder(root, include="*.jpg")
    assert len(third.uploaded) == 3
    fresh = [r for r in third.uploaded if not r.resumed]
    assert [r.metadata["relative_path"] for r in fresh] == ["a.jpg"]
    assert len(completed_records(manager)) == len(visible) + 1


async def test_upload_folder_filters(folder_server, tmp_path):
    server, _, _ = folder_server
    root = tmp_path / "tree"
    make_tree(root)
    async with AsyncUploader(f"{server.url}/uploads", retry=FAST_RETRY) as up:
        result = await up.upload_folder(
            root, include=["*.jpg", "*.txt"], exclude="sub/deeper/*", include_hidden=True
        )
    rels = sorted(r.metadata["relative_path"] for r in result.uploaded)
    assert rels == [".hidden/secret.jpg", "a.jpg", "sub/b.jpg"]
    with pytest.raises(NotADirectoryError):
        async with AsyncUploader(f"{server.url}/uploads") as up:
            await up.upload_folder(root / "a.jpg")


async def test_upload_many_reports_failures_per_file(batch_server, tmp_path):
    server, _, _ = batch_server
    good = tmp_path / "good.bin"
    good.write_bytes(os.urandom(5 * KiB))
    denied = tmp_path / "denied.bin"
    denied.write_bytes(b"no")
    finished = []
    async with AsyncUploader(
        f"{server.url}/uploads", headers={"X-User": "alice"}, retry=FAST_RETRY
    ) as up:
        result = await up.upload_many(
            [good, UploadItem(denied), tmp_path / "missing.bin", tmp_path],
            on_file=finished.append,
        )
    assert [r.path for r in result.uploaded] == [good]
    assert [f.path.name for f in result.failed] == ["denied.bin", "missing.bin", tmp_path.name]
    assert result.failed[0].error.code == "forbidden"
    assert isinstance(result.failed[1].error, FileNotFoundError)
    assert isinstance(result.failed[2].error, IsADirectoryError)
    assert len(finished) == 4 and sum(isinstance(f, FailedUpload) for f in finished) == 3
    with pytest.raises(BatchUploadError, match="3 file"):
        result.raise_for_failures()


async def test_upload_many_waits_when_server_limits_pending_uploads(tmp_path):
    manager = make_manager(tmp_path, max_pending_per_owner=2)
    app = FastAPI()
    app.include_router(create_upload_router(manager, principal=lambda r: "alice"))
    server = LiveServer(app)
    server.start()
    try:
        paths = []
        for i in range(9):
            path = tmp_path / f"f{i}.bin"
            path.write_bytes(os.urandom(2 * KiB + i))
            paths.append(path)
        async with AsyncUploader(f"{server.url}/uploads", retry=FAST_RETRY) as up:
            result = await up.upload_many(paths, file_concurrency=4, batch_size=5)
    finally:
        server.stop()
    result.raise_for_failures()
    assert len(result.uploaded) == 9 and len(completed_records(manager)) == 9


async def test_upload_many_falls_back_without_batch_endpoint(folder_server, tmp_path):
    server, manager, _ = folder_server
    # A proxy-like app in front of the real server that has no /batch route.
    calls = []
    inner = server.app

    async def no_batch(scope, receive, send):
        if scope["type"] == "http":
            calls.append((scope["method"], scope["path"]))
            if scope["path"].endswith("/batch"):
                response = JSONResponse({"detail": "Method Not Allowed"}, status_code=405)
                await response(scope, receive, send)
                return
        await inner(scope, receive, send)

    proxy = LiveServer(no_batch)
    proxy.start()
    try:
        paths = []
        for i in range(3):
            path = tmp_path / f"x{i}.bin"
            path.write_bytes(os.urandom(KiB))
            paths.append(path)
        async with AsyncUploader(f"{proxy.url}/uploads", retry=FAST_RETRY) as up:
            first = await up.upload_many(paths[:2])
            second = await up.upload_many(paths[2:])
    finally:
        proxy.stop()
    assert first.ok and second.ok
    assert sum(path.endswith("/batch") for _, path in calls) == 1  # remembered
    assert len(completed_records(manager)) == 3


async def test_interrupted_batch_resumes_without_duplicates(folder_server, tmp_path):
    server, manager, _ = folder_server
    root = tmp_path / "many"
    root.mkdir()
    for i in range(12):
        (root / f"{i:02d}.bin").write_bytes(os.urandom(20 * KiB))

    class Stop(Exception):
        pass

    def stop_after_four(progress):
        if progress.files_done >= 4:
            raise Stop

    async with AsyncUploader(f"{server.url}/uploads", concurrency=2, retry=FAST_RETRY) as up:
        with pytest.raises(Stop):
            await up.upload_folder(root, progress=stop_after_four, file_concurrency=2)
    done_before = len(completed_records(manager))
    assert 4 <= done_before < 12

    async with AsyncUploader(f"{server.url}/uploads", retry=FAST_RETRY) as up:
        result = await up.upload_folder(root)
    assert result.ok and len(result.uploaded) == 12
    assert sum(r.resumed for r in result.uploaded) >= done_before
    assert len(completed_records(manager)) == 12


def test_sync_upload_folder_and_cli(folder_server, tmp_path, capsys):
    from xferkit.cli import main as cli_main

    server, manager, _ = folder_server
    root = tmp_path / "docs"
    (root / "x").mkdir(parents=True)
    (root / "x" / "one.txt").write_bytes(b"1" * 1000)
    (root / "two.txt").write_bytes(b"2" * 2000)
    result = upload_folder(f"{server.url}/uploads", root, include="*.txt", concurrency=2)
    assert result.ok and len(result.uploaded) == 2

    (root / "three.txt").write_bytes(b"3" * 3000)
    code = cli_main(["upload", str(root), f"{server.url}/uploads", "-q"])
    out = capsys.readouterr().out
    assert code == 0
    assert "3 uploaded" in out and "0 failed" in out
    assert len(completed_records(manager)) == 3
