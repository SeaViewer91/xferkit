from email.utils import formatdate

import pytest

from xferkit.errors import RangeNotSatisfiable
from xferkit.server.download import (
    ByteRange,
    content_disposition,
    iter_file_range,
    parse_range_header,
    plan_file_response,
)


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        (None, None),
        ("", None),
        ("bytes=0-9", ByteRange(0, 9)),
        ("bytes=10-", ByteRange(10, 99)),
        ("bytes=-10", ByteRange(90, 99)),
        ("bytes=-1000", ByteRange(0, 99)),
        ("bytes=50-1000", ByteRange(50, 99)),
        ("bytes=0-1,5-6", None),  # multi-range: serve everything
        ("items=0-1", None),
        ("bytes=abc", None),
        ("bytes=9-3", None),
    ],
)
def test_parse_range_header(header, expected):
    assert parse_range_header(header, 100) == expected


@pytest.mark.parametrize("header", ["bytes=100-", "bytes=200-300", "bytes=-0"])
def test_parse_range_header_unsatisfiable(header):
    with pytest.raises(RangeNotSatisfiable):
        parse_range_header(header, 100)


def test_content_disposition_unicode():
    value = content_disposition("해양 영상.tif")
    assert value.startswith('attachment; filename="')
    assert "filename*=UTF-8''%ED%95%B4%EC%96%91%20%EC%98%81%EC%83%81.tif" in value
    assert content_disposition("plain.txt", "inline") == 'inline; filename="plain.txt"'


@pytest.fixture
def sample(tmp_path):
    path = tmp_path / "sample.bin"
    path.write_bytes(bytes(range(256)) * 4)  # 1024 bytes
    return path


def test_plan_full_and_range(sample):
    full = plan_file_response(sample)
    assert full.status == 200 and full.length == 1024
    assert full.headers["Accept-Ranges"] == "bytes"

    part = plan_file_response(sample, request_headers={"Range": "bytes=100-199"})
    assert part.status == 206
    assert part.headers["Content-Range"] == "bytes 100-199/1024"
    assert (
        b"".join(iter_file_range(sample, part.start, part.length, 7))
        == sample.read_bytes()[100:200]
    )


def test_plan_if_range(sample):
    etag = plan_file_response(sample).etag
    ok = plan_file_response(sample, request_headers={"range": "bytes=0-0", "if-range": etag})
    assert ok.status == 206
    stale = plan_file_response(sample, request_headers={"range": "bytes=0-0", "if-range": '"x"'})
    assert stale.status == 200
    date = formatdate(sample.stat().st_mtime, usegmt=True)
    by_date = plan_file_response(sample, request_headers={"range": "bytes=0-0", "if-range": date})
    assert by_date.status == 206


def test_plan_not_modified_and_416_and_head(sample):
    etag = plan_file_response(sample).etag
    assert plan_file_response(sample, request_headers={"If-None-Match": etag}).status == 304
    bad = plan_file_response(sample, request_headers={"Range": "bytes=5000-"})
    assert bad.status == 416 and bad.headers["Content-Range"] == "bytes */1024"
    head = plan_file_response(sample, method="HEAD")
    assert head.status == 200 and head.send_body is False


def test_plan_missing_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        plan_file_response(tmp_path / "nope")
