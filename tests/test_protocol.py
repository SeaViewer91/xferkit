import hashlib

import pytest

from xferkit.errors import UploadIncomplete, error_from_response
from xferkit.protocol import (
    GiB,
    KiB,
    MiB,
    composite_checksum,
    format_checksum,
    parse_checksum,
    parse_size,
    total_parts,
)


def test_checksum_round_trip():
    digest = hashlib.sha256(b"x").hexdigest()
    value = format_checksum(digest.upper())
    assert value == f"sha256={digest}"
    assert parse_checksum(f" SHA256 = {digest.upper()} ") == ("sha256", digest)


@pytest.mark.parametrize("value", ["", "md5=abc", "sha256=xyz", "sha256=" + "0" * 63])
def test_parse_checksum_rejects(value):
    with pytest.raises(ValueError):
        parse_checksum(value)


def test_total_parts():
    assert total_parts(0, 10) == 0
    assert total_parts(1, 10) == 1
    assert total_parts(10, 10) == 1
    assert total_parts(11, 10) == 2


def test_composite_checksum_matches_definition():
    parts = [b"a" * 3, b"b" * 5]
    digests = [hashlib.sha256(p).digest() for p in parts]
    expected = hashlib.sha256(b"".join(digests)).hexdigest()
    assert composite_checksum(digests) == f"sha256={expected}"


@pytest.mark.parametrize(
    ("text", "value"),
    [("1024", 1024), ("8M", 8 * MiB), ("512KiB", 512 * KiB), ("1.5g", int(1.5 * GiB)), (7, 7)],
)
def test_parse_size(text, value):
    assert parse_size(text) == value


def test_error_round_trip():
    original = UploadIncomplete("missing", details={"missing_parts": [1, 2]})
    rebuilt = error_from_response(409, original.to_dict())
    assert isinstance(rebuilt, UploadIncomplete)
    assert rebuilt.details == {"missing_parts": [1, 2]}
    assert rebuilt.status_code == 409
