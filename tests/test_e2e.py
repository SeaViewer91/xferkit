"""End-to-end tests: real uvicorn server + xferkit client over HTTP."""

import hashlib

import httpx
import pytest

from xferkit import ChecksumMismatch, UploadNotFound
from xferkit.cli import main as cli_main
from xferkit.client import (
    AsyncDownloader,
    AsyncUploader,
    RetryPolicy,
    download_file,
    upload_file,
)
from xferkit.protocol import CHECKSUM_HEADER, PROTOCOL_HEADER, KiB, format_checksum

FAST_RETRY = RetryPolicy(attempts=3, backoff=0.01, max_backoff=0.05)


class Stop(Exception):
    pass


def stop_after(nbytes):
    def callback(progress):
        if progress.bytes_done >= nbytes:
            raise Stop

    return callback


async def test_parallel_upload_round_trip(live_server, app, random_file):
    source = random_file("수심 영상.tif", 1_000_003)
    seen = []
    async with AsyncUploader(
        f"{live_server.url}/uploads", chunk_size=64 * KiB, concurrency=6, retry=FAST_RETRY
    ) as uploader:
        result = await uploader.upload(
            source, metadata={"project": "KHOA"}, progress=lambda p: seen.append(p.bytes_done)
        )

    assert result.size == source.stat().st_size
    assert not result.resumed
    assert seen[-1] == result.size
    [completed] = app.state.completed
    assert completed.upload_id == result.upload_id
    assert completed.filename == "수심 영상.tif"
    assert completed.metadata == {"project": "KHOA"}
    assert completed.path.read_bytes() == source.read_bytes()


async def test_upload_with_whole_file_checksum(live_server, app, random_file):
    source = random_file("a.bin", 200_000)
    async with AsyncUploader(f"{live_server.url}/uploads", send_checksum=True) as uploader:
        result = await uploader.upload(source)
    assert result.checksum == format_checksum(hashlib.sha256(source.read_bytes()).hexdigest())


async def test_upload_resumes_from_journal(live_server, app, random_file, tmp_path):
    source = random_file("resume.bin", 640 * KiB)
    state = tmp_path / "state"
    options = dict(chunk_size=64 * KiB, concurrency=1, state_dir=state, retry=FAST_RETRY)
    base = f"{live_server.url}/uploads"

    async with AsyncUploader(base, **options) as uploader:
        with pytest.raises(Stop):
            await uploader.upload(source, progress=stop_after(256 * KiB))
    assert list(state.iterdir())  # journal kept for resuming

    sent = []
    async with AsyncUploader(base, **options) as uploader:
        result = await uploader.upload(source, progress=lambda p: sent.append(p.bytes_done))
    assert result.resumed
    # The first report already counts the 4 parts sent before the interruption,
    # and only the 6 remaining parts are sent again.
    assert sent[0] == 256 * KiB
    assert len(sent) == 1 + 6
    assert app.state.completed[0].path.read_bytes() == source.read_bytes()
    assert not list(state.iterdir())  # journal removed after success


async def test_upload_resumes_explicit_session(live_server, app, random_file):
    source = random_file("explicit.bin", 300 * KiB)
    data = source.read_bytes()
    base = f"{live_server.url}/uploads"
    async with httpx.AsyncClient() as http:
        created = (
            await http.post(
                base, json={"filename": "explicit.bin", "size": len(data), "chunk_size": 100 * KiB}
            )
        ).json()
        uid = created["upload_id"]
        assert created["total_parts"] == 3
        response = await http.put(f"{base}/{uid}/parts/1", content=data[100 * KiB : 200 * KiB])
        assert response.status_code == 200

    async with AsyncUploader(base) as uploader:
        result = await uploader.upload(source, upload_id=uid)
    assert result.resumed and result.upload_id == uid
    assert app.state.completed[0].path.read_bytes() == data

    async with AsyncUploader(base) as uploader:
        with pytest.raises(UploadNotFound):
            await uploader.upload(source, upload_id="doesNotExist123")


async def test_protocol_errors_over_http(live_server):
    base = f"{live_server.url}/uploads"
    async with httpx.AsyncClient() as http:
        created = await http.post(base, json={"filename": "x", "size": 2048, "chunk_size": 1024})
        assert created.status_code == 201
        assert created.headers[PROTOCOL_HEADER] == "1"
        assert created.headers["location"].endswith(created.json()["upload_id"])
        uid = created.json()["upload_id"]

        wrong_sum = await http.put(
            f"{base}/{uid}/parts/0",
            content=b"a" * 1024,
            headers={CHECKSUM_HEADER: format_checksum("0" * 64)},
        )
        assert wrong_sum.status_code == 422
        assert wrong_sum.json()["error"]["code"] == ChecksumMismatch.code

        short = await http.put(f"{base}/{uid}/parts/0", content=b"a" * 10)
        assert short.status_code == 400

        incomplete = await http.post(f"{base}/{uid}/complete")
        assert incomplete.status_code == 409
        assert incomplete.json()["error"]["details"]["missing_parts"] == [0, 1]

        assert (await http.get(f"{base}/nope_nope_nope")).status_code == 404
        assert (await http.post(base, json={"filename": "x", "size": -1})).status_code == 422
        assert (await http.delete(f"{base}/{uid}")).status_code == 204
        assert (await http.get(f"{base}/{uid}")).status_code == 404


async def test_parallel_ranged_download(live_server, app, random_file, tmp_path):
    source = random_file("big.bin", 1_234_567, directory=app.state.public)
    dest = tmp_path / "out" / "big.bin"
    async with AsyncDownloader(
        concurrency=5, segment_size=100 * KiB, retry=FAST_RETRY
    ) as downloader:
        result = await downloader.download(
            f"{live_server.url}/files/big.bin",
            dest,
            checksum=format_checksum(hashlib.sha256(source.read_bytes()).hexdigest()),
        )
    assert result.ranged and not result.resumed
    assert dest.read_bytes() == source.read_bytes()
    assert not dest.with_name("big.bin.xferkit-part").exists()


async def test_download_resumes(live_server, app, random_file, tmp_path):
    source = random_file("r.bin", 800 * KiB, directory=app.state.public)
    dest = tmp_path / "r.bin"
    url = f"{live_server.url}/files/r.bin"
    options = dict(concurrency=1, segment_size=64 * KiB, retry=FAST_RETRY)

    async with AsyncDownloader(**options) as downloader:
        with pytest.raises(Stop):
            await downloader.download(url, dest, progress=stop_after(300 * KiB))
    assert dest.with_name("r.bin.xferkit-state").exists()

    progress = []
    async with AsyncDownloader(**options) as downloader:
        result = await downloader.download(
            url, dest, progress=lambda p: progress.append(p.bytes_done)
        )
    assert result.resumed
    assert progress[0] >= 256 * KiB  # finished segments are not fetched again
    assert dest.read_bytes() == source.read_bytes()


async def test_download_restarts_when_remote_changed(live_server, app, random_file, tmp_path):
    source = random_file("c.bin", 512 * KiB, directory=app.state.public)
    dest = tmp_path / "c.bin"
    url = f"{live_server.url}/files/c.bin"
    options = dict(concurrency=1, segment_size=64 * KiB, retry=FAST_RETRY)
    async with AsyncDownloader(**options) as downloader:
        with pytest.raises(Stop):
            await downloader.download(url, dest, progress=stop_after(128 * KiB))

    new_content = bytes(reversed(source.read_bytes())) + b"tail"
    source.write_bytes(new_content)
    async with AsyncDownloader(**options) as downloader:
        result = await downloader.download(url, dest)
    assert not result.resumed
    assert dest.read_bytes() == new_content


async def test_download_without_range_support(live_server, app, random_file, tmp_path):
    source = random_file("p.bin", 300 * KiB, directory=app.state.public)
    dest = tmp_path / "p.bin"
    async with AsyncDownloader() as downloader:
        result = await downloader.download(f"{live_server.url}/plain/p.bin", dest)
    assert not result.ranged
    assert dest.read_bytes() == source.read_bytes()


async def test_download_completed_upload_and_empty_file(live_server, app, random_file, tmp_path):
    source = random_file("한글.bin", 150 * KiB)
    async with AsyncUploader(f"{live_server.url}/uploads") as uploader:
        result = await uploader.upload(source)
    url = f"{live_server.url}/uploads/{result.upload_id}/content"
    async with httpx.AsyncClient() as http:
        head = await http.head(url)
        assert head.status_code == 200
        assert "filename*=UTF-8''%ED%95%9C%EA%B8%80.bin" in head.headers["content-disposition"]
    async with AsyncDownloader(segment_size=40 * KiB) as downloader:
        await downloader.download(url, tmp_path / "copy.bin")
    assert (tmp_path / "copy.bin").read_bytes() == source.read_bytes()

    (app.state.public / "empty").write_bytes(b"")
    async with AsyncDownloader() as downloader:
        empty = await downloader.download(f"{live_server.url}/files/empty", tmp_path / "empty")
    assert empty.size == 0 and (tmp_path / "empty").read_bytes() == b""


def test_sync_api_and_cli(live_server, app, random_file, tmp_path, capsys):
    source = random_file("sync.bin", 200 * KiB)
    result = upload_file(f"{live_server.url}/uploads", source, chunk_size=32 * KiB)
    downloaded = download_file(
        f"{live_server.url}/uploads/{result.upload_id}/content", tmp_path / "sync.out"
    )
    assert downloaded.path.read_bytes() == source.read_bytes()

    code = cli_main(
        [
            "upload",
            str(source),
            f"{live_server.url}/uploads",
            "--state-dir",
            str(tmp_path / "st"),
            "-q",
            "--meta",
            "k=v",
            "--checksum",
        ]
    )
    assert code == 0
    uid = capsys.readouterr().out.split()[0].split("=")[1]
    code = cli_main(
        ["download", f"{live_server.url}/uploads/{uid}/content", str(tmp_path / "cli.out"), "-q"]
    )
    assert code == 0
    assert (tmp_path / "cli.out").read_bytes() == source.read_bytes()
    assert (
        cli_main(["download", f"{live_server.url}/files/missing", str(tmp_path / "m"), "-q"]) == 1
    )
