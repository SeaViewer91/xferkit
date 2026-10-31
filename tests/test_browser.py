"""The browser client in a real browser (Chromium via Playwright).

The page and the transfer API are served from two different origins, so CORS,
grants and the bundled ES module are exercised exactly as in production.
Skipped when Playwright or its Chromium build is not available.
"""

import hashlib
import os

import pytest
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, Response

from conftest import LiveServer
from xferkit import GrantSigner, LocalStorage, UploadManager
from xferkit.integrations.fastapi import add_cors, create_upload_router, mount_browser_client
from xferkit.protocol import KiB

playwright_api = pytest.importorskip("playwright.async_api")

SECRET = "b" * 32
PAGE = """<!doctype html><meta charset="utf-8"><title>xferkit browser test</title>
<script type="module">
  import * as xferkit from "/xferkit.mjs";
  window.xferkit = xferkit;
  window.xferkitReady = true;
</script>"""


@pytest.fixture
def sample():
    return os.urandom(700 * KiB + 3)


@pytest.fixture
def servers(tmp_path, sample):
    manager = UploadManager(
        LocalStorage(tmp_path / "store", min_free_bytes=0),
        min_chunk_size=1 * KiB,
        default_chunk_size=64 * KiB,
    )
    signer = GrantSigner(SECRET)
    completed = []

    frontend = FastAPI()
    mount_browser_client(frontend)

    @frontend.get("/", response_class=HTMLResponse)
    async def page():
        return PAGE

    @frontend.get("/sample.bin")
    async def sample_file():
        return Response(sample, media_type="application/octet-stream")

    front = LiveServer(frontend)
    front.start()

    api = FastAPI()
    api.include_router(
        create_upload_router(
            manager,
            grants=signer,
            principal=lambda request: None,
            require_auth=True,
            expose_downloads=True,
            on_complete=lambda record, request: completed.append(record),
        )
    )
    add_cors(api, [front.url])
    back = LiveServer(api)
    back.start()
    yield {
        "front": front,
        "api": back,
        "manager": manager,
        "signer": signer,
        "completed": completed,
        "store": tmp_path / "store",
    }
    back.stop()
    front.stop()


@pytest.fixture
async def page(servers):
    async with playwright_api.async_playwright() as pw:
        try:
            browser = await pw.chromium.launch()
        except Exception as exc:  # pragma: no cover - environment without browsers
            pytest.skip(f"Chromium not available: {exc}")
        page = await browser.new_page()
        errors = []
        page.on("pageerror", lambda exc: errors.append(str(exc)))
        await page.goto(servers["front"].url)
        await page.wait_for_function("window.xferkitReady === true")
        yield page
        await browser.close()
        assert not errors, errors


def upload_grant(servers, owner="alice"):
    return servers["signer"].issue(owner=owner, prefix="/uploads", methods="*", ttl=600)


async def test_upload_from_browser(page, servers, sample):
    result = await page.evaluate(
        """async ({ api, grant }) => {
            const blob = await (await fetch("/sample.bin")).blob();
            const file = new File([blob], "드론 원본.bin", { lastModified: 1 });
            const seen = [];
            const result = await xferkit.upload(file, {
                baseUrl: api + "/uploads", grant, chunkSize: 64 * 1024, concurrency: 4,
                metadata: { site: "부산" }, onProgress: (p) => seen.push(p.fraction),
            });
            return { ...result, progress: seen };
        }""",
        {"api": servers["api"].url, "grant": upload_grant(servers)},
    )
    assert not result["resumed"]
    assert result["progress"][-1] == 1
    [record] = servers["completed"]
    assert record.upload_id == result["uploadId"]
    assert record.filename == "드론 원본.bin"
    assert record.owner == "alice" and record.metadata == {"site": "부산"}
    assert record.path.read_bytes() == sample


async def test_browser_upload_resumes_after_abort(page, servers, sample):
    result = await page.evaluate(
        """async ({ api, grant }) => {
            const blob = await (await fetch("/sample.bin")).blob();
            const file = new File([blob], "resume.bin", { lastModified: 2 });
            const options = { baseUrl: api + "/uploads", grant, chunkSize: 16 * 1024, concurrency: 2 };
            const controller = new AbortController();
            let aborted = null;
            try {
                await xferkit.upload(file, { ...options, signal: controller.signal,
                    onProgress: (p) => { if (p.fraction > 0.3) controller.abort(); } });
            } catch (error) {
                aborted = error.name;
            }
            const first = [];
            const result = await xferkit.upload(file, { ...options,
                onProgress: (p) => first.push(p.bytesDone) });
            return { aborted, resumed: result.resumed, uploadId: result.uploadId,
                     firstReport: first[0], journal: Object.keys(localStorage) };
        }""",
        {"api": servers["api"].url, "grant": upload_grant(servers)},
    )
    assert result["aborted"] == "AbortError"
    assert result["resumed"]
    assert result["firstReport"] > 0.3 * len(sample)  # finished parts were not sent again
    assert result["journal"] == []  # cleaned up after success
    assert len(servers["completed"]) == 1
    assert servers["completed"][0].path.read_bytes() == sample
    assert list((servers["store"] / "uploads").iterdir()) == []  # no orphaned session


async def test_idempotency_key_matches_python_client(page, servers, sample, tmp_path):
    """The same key means the same upload, from JavaScript or from Python."""
    from xferkit.client import AsyncUploader

    path = tmp_path / "shared.bin"
    path.write_bytes(sample)
    grant = upload_grant(servers)
    async with AsyncUploader(f"{servers['api'].url}/uploads", grant=grant) as uploader:
        py = await uploader.upload(path, idempotency_key="survey-7/shared.bin")
    js = await page.evaluate(
        """async ({ api, grant }) => {
            const blob = await (await fetch("/sample.bin")).blob();
            const file = new File([blob], "shared.bin");
            return await xferkit.upload(file, { baseUrl: api + "/uploads", grant,
                idempotencyKey: "survey-7/shared.bin", resume: false });
        }""",
        {"api": servers["api"].url, "grant": grant},
    )
    assert js["uploadId"] == py.upload_id and js["resumed"]
    assert len(servers["completed"]) == 1


async def test_download_in_browser(page, servers, sample):
    manager = servers["manager"]
    session = manager.create_upload("dl.bin", len(sample), chunk_size=64 * KiB, owner="alice")
    for i in range(session.total_parts):
        start = session.part_offset(i)
        manager.put_part(session.upload_id, i, sample[start : start + session.part_size(i)])
    manager.complete(session.upload_id)
    link = servers["signer"].sign_url(
        f"{servers['api'].url}/uploads/{session.upload_id}/content", ttl=60
    )
    checksum = "sha256=" + hashlib.sha256(sample).hexdigest()
    result = await page.evaluate(
        """async ({ link, checksum, size }) => {
            const hex = (buf) => [...new Uint8Array(buf)].map(b => b.toString(16).padStart(2, "0")).join("");
            const blob = await xferkit.download(link, { segmentSize: 64 * 1024, concurrency: 4, checksum });
            const viaBlob = hex(await crypto.subtle.digest("SHA-256", await blob.arrayBuffer()));
            // Positional writer, as returned by FileSystemFileHandle.createWritable().
            const buffer = new Uint8Array(size);
            let closed = false;
            const writer = {
                async write({ position, data }) { buffer.set(new Uint8Array(data), position); },
                async close() { closed = true; },
            };
            await xferkit.download(link, { segmentSize: 50 * 1024, target: writer });
            const viaWriter = hex(await crypto.subtle.digest("SHA-256", buffer));
            return { viaBlob, viaWriter, closed, type: blob.type, size: blob.size };
        }""",
        {"link": link, "checksum": checksum, "size": len(sample)},
    )
    expected = hashlib.sha256(sample).hexdigest()
    assert result["viaBlob"] == expected and result["viaWriter"] == expected
    assert result["closed"] and result["size"] == len(sample)


async def test_browser_errors_carry_server_codes(page, servers):
    result = await page.evaluate(
        """async ({ api, forged }) => {
            const file = new File([new Uint8Array(10)], "x.bin");
            const out = {};
            try {
                await xferkit.upload(file, { baseUrl: api + "/uploads", grant: forged, resume: false });
            } catch (error) {
                Object.assign(out, { name: error.name, code: error.code, status: error.status });
            }
            return out;
        }""",
        {
            "api": servers["api"].url,
            "forged": GrantSigner("z" * 32).issue(owner="alice", prefix="/", methods="*"),
        },
    )
    assert result == {"name": "XferkitError", "code": "invalid_grant", "status": 401}


async def test_bundle_is_served_as_javascript(servers):
    import httpx

    async with httpx.AsyncClient() as http:
        r = await http.get(f"{servers['front'].url}/xferkit.mjs")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/javascript")
    assert "export" in r.text


def test_anonymous_calls_are_rejected(servers):
    # principal() returns None and no grant is sent: require_auth answers 401.
    import httpx

    r = httpx.post(f"{servers['api'].url}/uploads", json={"filename": "a", "size": 1})
    assert r.status_code == 401


async def test_folder_upload_from_browser_matches_python_keys(page, servers, tmp_path):
    from xferkit.client import AsyncUploader

    root = tmp_path / "tree"
    contents = {
        "a.jpg": os.urandom(3 * KiB),
        "sub/b.jpg": os.urandom(150 * KiB),  # several parts
        "sub/deeper/c.txt": b"hello",
        ".DS_Store": b"junk",
    }
    for rel, data in contents.items():
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        (root / rel).write_bytes(data)

    await page.evaluate(
        """() => {
            const input = document.createElement("input");
            input.type = "file";
            input.webkitdirectory = true;
            input.id = "folder";
            document.body.append(input);
        }"""
    )
    await page.set_input_files("#folder", str(root))
    grant = upload_grant(servers)
    result = await page.evaluate(
        """async ({ api, grant }) => {
            const input = document.getElementById("folder");
            const seen = [];
            const files = [];
            const r = await xferkit.uploadFiles(input.files, {
                baseUrl: api + "/uploads", grant, chunkSize: 64 * 1024,
                metadata: { survey: "S1" },
                onProgress: (p) => seen.push(p), onFile: (f) => files.push(f.relativePath),
            });
            return { ...r, last: seen[seen.length - 1], files };
        }""",
        {"api": servers["api"].url, "grant": grant},
    )
    assert result["ok"], result["failed"]
    rels = sorted(u["relativePath"] for u in result["uploaded"])
    assert rels == ["a.jpg", "sub/b.jpg", "sub/deeper/c.txt"]  # hidden file skipped
    assert result["last"]["filesDone"] == 3 and result["last"]["fraction"] == 1
    by_rel = {c.metadata["relative_path"]: c for c in servers["completed"]}
    for rel in rels:
        assert by_rel[rel].path.read_bytes() == contents[rel]
        assert by_rel[rel].metadata["survey"] == "S1"
        assert by_rel[rel].owner == "alice"

    # The Python client derives the same keys for the same folder: nothing new.
    async with AsyncUploader(f"{servers['api'].url}/uploads", grant=grant) as up:
        again = await up.upload_folder(root, key_prefix="tree/")
    assert again.ok and len(again.uploaded) == 3
    assert all(r.resumed for r in again.uploaded)
    assert len(servers["completed"]) == 3


async def test_browser_batch_reports_failures_per_file(page, servers):
    small = servers["signer"].issue(
        owner="bob", prefix="/uploads", methods="*", ttl=600, max_size=1000
    )
    result = await page.evaluate(
        """async ({ api, grant }) => {
            const entries = [
                { file: new File(["x".repeat(10)], "ok.txt"), folder: "set", relativePath: "ok.txt" },
                { file: new File(["y".repeat(5000)], "big.txt"), folder: "set", relativePath: "d/big.txt" },
            ];
            const r = await xferkit.uploadFiles(entries, { baseUrl: api + "/uploads", grant });
            return {
                ok: r.ok,
                uploaded: r.uploaded.map((u) => u.relativePath),
                failed: r.failed.map((f) => [f.relativePath, f.error.code, f.error.status]),
            };
        }""",
        {"api": servers["api"].url, "grant": small},
    )
    assert result == {
        "ok": False,
        "uploaded": ["ok.txt"],
        "failed": [["d/big.txt", "payload_too_large", 413]],
    }
