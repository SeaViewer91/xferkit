"""Authentication, ownership, grants, rate limiting and CORS over real HTTP."""

import httpx
import pytest
from fastapi import FastAPI, Request

from conftest import LiveServer
from xferkit import GrantSigner, LocalStorage, RateLimiter, UploadManager, UploadNotFound
from xferkit.client import AsyncDownloader, AsyncUploader, RetryPolicy
from xferkit.grants import GRANT_HEADER
from xferkit.integrations.fastapi import add_cors, create_upload_router
from xferkit.protocol import KiB

SECRET = "x" * 32
FAST_RETRY = RetryPolicy(attempts=4, backoff=0.01, max_backoff=0.05)


def principal_from_header(request: Request):
    # Stand-in for real authentication (session cookie, JWT, ...).
    return request.headers.get("X-User")


async def authorize(request, ctx):
    return not (ctx.caller.owner == "auditor" and ctx.action in ("create", "abort"))


@pytest.fixture
def secured(tmp_path):
    manager = UploadManager(
        LocalStorage(tmp_path / "store", min_free_bytes=0),
        min_chunk_size=1 * KiB,
        default_chunk_size=64 * KiB,
    )
    signer = GrantSigner(SECRET)
    completed = []
    app = FastAPI()
    app.include_router(
        create_upload_router(
            manager,
            principal=principal_from_header,
            grants=signer,
            require_auth=True,
            authorize=authorize,
            expose_downloads=True,
            on_complete=lambda record, request: completed.append(
                (record, request.state.xferkit_caller)
            ),
        )
    )
    add_cors(app, ["http://frontend.example"])
    server = LiveServer(app)
    server.start()
    yield server, signer, completed
    server.stop()


def user(name):
    return {"X-User": name}


async def test_anonymous_requests_are_rejected(secured):
    server, _, _ = secured
    async with httpx.AsyncClient(base_url=server.url) as http:
        r = await http.post("/uploads", json={"filename": "a", "size": 1})
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "unauthorized"


async def test_uploads_are_private_to_their_owner(secured, random_file, tmp_path):
    server, _, completed = secured
    source = random_file("private.bin", 150 * KiB)
    async with AsyncUploader(f"{server.url}/uploads", headers=user("alice")) as uploader:
        result = await uploader.upload(source)
    record, caller = completed[0]
    assert record.owner == "alice" and caller.owner == "alice"
    assert result.response["owner"] == "alice"

    base = f"/uploads/{result.upload_id}"
    async with httpx.AsyncClient(base_url=server.url) as http:
        assert (await http.get(base, headers=user("alice"))).json()["state"] == "completed"
        for path in (base, f"{base}/content"):  # not even existence is revealed
            r = await http.get(path, headers=user("bob"))
            assert r.status_code == 404
        r = await http.post(f"{base}/complete", headers=user("bob"))
        assert r.status_code == 404

    async with AsyncDownloader(headers=user("alice")) as downloader:
        await downloader.download(f"{server.url}{base}/content", tmp_path / "back.bin")
    assert (tmp_path / "back.bin").read_bytes() == source.read_bytes()


async def test_parts_of_someone_elses_upload_are_rejected(secured):
    server, _, _ = secured
    async with httpx.AsyncClient(base_url=server.url) as http:
        created = await http.post(
            "/uploads", json={"filename": "a", "size": 2048, "chunk_size": 1024},
            headers=user("alice"),
        )  # fmt: skip
        uid = created.json()["upload_id"]
        r = await http.put(f"/uploads/{uid}/parts/0", content=b"x" * 1024, headers=user("bob"))
        assert r.status_code == 404
        r = await http.put(f"/uploads/{uid}/parts/0", content=b"x" * 1024, headers=user("alice"))
        assert r.status_code == 200
        assert (await http.delete(f"/uploads/{uid}", headers=user("bob"))).status_code == 404
        assert (await http.delete(f"/uploads/{uid}", headers=user("alice"))).status_code == 204


async def test_authorize_hook(secured):
    server, _, _ = secured
    async with httpx.AsyncClient(base_url=server.url) as http:
        r = await http.post("/uploads", json={"filename": "a", "size": 1}, headers=user("auditor"))
    assert r.status_code == 403 and r.json()["error"]["code"] == "forbidden"


async def test_upload_and_download_with_grants_only(secured, random_file, tmp_path):
    """A browser or worker with no account: the application hands it grants."""
    server, signer, completed = secured
    upload_grant = signer.issue(owner="alice", prefix="/uploads", methods="*", ttl=600)
    source = random_file("granted.bin", 200 * KiB)
    async with AsyncUploader(f"{server.url}/uploads", grant=upload_grant) as uploader:
        result = await uploader.upload(source)
    assert completed[0][0].owner == "alice"
    assert completed[0][1].grant is not None

    # A download link for exactly this file, usable without any other credential.
    link = signer.sign_url(f"{server.url}/uploads/{result.upload_id}/content", ttl=60)
    async with AsyncDownloader(segment_size=64 * KiB) as downloader:
        await downloader.download(link, tmp_path / "via-link.bin")
    assert (tmp_path / "via-link.bin").read_bytes() == source.read_bytes()

    async with httpx.AsyncClient(base_url=server.url) as http:
        other = await http.post("/uploads", json={"filename": "z", "size": 1}, headers=user("bob"))
        other_id = other.json()["upload_id"]
        # The link is scoped to one path and read-only.
        token = link.split("xk=")[1]
        r = await http.get(f"/uploads/{other_id}", params={"xk": token})
        assert r.status_code == 401 and r.json()["error"]["code"] == "invalid_grant"
        r = await http.delete(f"/uploads/{result.upload_id}", params={"xk": token})
        assert r.status_code == 401
        # A forged or foreign grant is rejected.
        foreign = GrantSigner("y" * 32).issue(owner="alice", prefix="/", methods="*")
        r = await http.post(
            "/uploads", json={"filename": "a", "size": 1}, headers={GRANT_HEADER: foreign}
        )
        assert r.status_code == 401


async def test_grant_size_limit(secured):
    server, signer, _ = secured
    grant = signer.issue(owner="alice", prefix="/uploads", methods="*", max_size=1000)
    async with httpx.AsyncClient(base_url=server.url, headers={GRANT_HEADER: grant}) as http:
        assert (
            await http.post("/uploads", json={"filename": "a", "size": 1000})
        ).status_code == 201
        r = await http.post("/uploads", json={"filename": "b", "size": 1001})
    assert r.status_code == 413


async def test_cors_preflight_and_exposed_headers(secured):
    server, _, _ = secured
    async with httpx.AsyncClient(base_url=server.url) as http:
        pre = await http.options(
            "/uploads/abc/parts/0",
            headers={
                "Origin": "http://frontend.example",
                "Access-Control-Request-Method": "PUT",
                "Access-Control-Request-Headers": "x-xferkit-checksum, x-xferkit-grant",
            },
        )
        assert pre.status_code == 200
        assert "x-xferkit-grant" in pre.headers["access-control-allow-headers"].lower()
        r = await http.post(
            "/uploads",
            json={"filename": "a", "size": 1},
            headers={"Origin": "http://frontend.example", **user("alice")},
        )
        exposed = r.headers["access-control-expose-headers"].lower()
        assert "location" in exposed and "content-range" in exposed
        evil = await http.options(
            "/uploads",
            headers={"Origin": "http://evil.example", "Access-Control-Request-Method": "POST"},
        )
        assert "access-control-allow-origin" not in evil.headers


async def test_rate_limit_answers_429_and_client_backs_off(tmp_path, random_file):
    manager = UploadManager(
        LocalStorage(tmp_path / "store", min_free_bytes=0),
        min_chunk_size=1 * KiB,
        default_chunk_size=16 * KiB,
    )
    app = FastAPI()
    app.include_router(create_upload_router(manager, rate_limit=RateLimiter(rate=5, burst=3)))
    server = LiveServer(app)
    server.start()
    try:
        async with httpx.AsyncClient(base_url=server.url) as http:
            codes = [(await http.get("/uploads/nope_nope_nope")).status_code for _ in range(8)]
            assert 429 in codes
            limited = await http.get("/uploads/nope_nope_nope")
            if limited.status_code == 429:
                assert int(limited.headers["retry-after"]) >= 1
        source = random_file("limited.bin", 200 * KiB)  # ~15 requests > burst of 3
        async with AsyncUploader(f"{server.url}/uploads", concurrency=4) as uploader:
            result = await uploader.upload(source)  # retries 429 using Retry-After
        assert manager.get_completed(result.upload_id).size == 200 * KiB
    finally:
        server.stop()


def test_owner_check_helper_rejects_strangers(tmp_path):
    manager = UploadManager(LocalStorage(tmp_path / "s", min_free_bytes=0))
    session = manager.create_upload("a", 1, owner="alice")
    with pytest.raises(UploadNotFound):
        manager.ensure_access(session.upload_id, "mallory")
