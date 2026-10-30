"""Wire-protocol constants and helpers shared by the server and the client.

The xferkit upload protocol is a small, S3-multipart-like HTTP protocol::

    POST   {base}                       create an upload session
    GET    {base}/{upload_id}           session status (received / missing parts)
    PUT    {base}/{upload_id}/parts/{n} upload part ``n`` (raw body)
    POST   {base}/{upload_id}/complete  assemble and verify the file
    DELETE {base}/{upload_id}           abort and discard the session

See ``docs/PROTOCOL.md`` for the full specification.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable

PROTOCOL_VERSION = "1"
PROTOCOL_HEADER = "X-Xferkit-Protocol"
CHECKSUM_HEADER = "X-Xferkit-Checksum"

KiB = 1024
MiB = 1024 * KiB
GiB = 1024 * MiB

DEFAULT_CHUNK_SIZE = 8 * MiB
DEFAULT_MIN_CHUNK_SIZE = 256 * KiB
DEFAULT_MAX_CHUNK_SIZE = 256 * MiB
DEFAULT_MAX_PARTS = 100_000

CHECKSUM_ALGORITHM = "sha256"

_CHECKSUM_RE = re.compile(r"^\s*(sha256)\s*=\s*([0-9a-f]{64})\s*$", re.IGNORECASE)
_SIZE_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([kmgt]?)(i?b?)\s*$", re.IGNORECASE)
_SIZE_UNITS = {"": 1, "k": KiB, "m": MiB, "g": GiB, "t": 1024 * GiB}


def format_checksum(hexdigest: str, algorithm: str = CHECKSUM_ALGORITHM) -> str:
    """Return a checksum in wire format, e.g. ``sha256=9f86d0...``."""
    return f"{algorithm}={hexdigest.lower()}"


def parse_checksum(value: str) -> tuple[str, str]:
    """Parse ``sha256=<hex>`` into ``("sha256", "<lowercase hex>")``.

    Raises:
        ValueError: if the value is not a supported checksum.
    """
    match = _CHECKSUM_RE.match(value or "")
    if match is None:
        raise ValueError(f"unsupported checksum {value!r}; expected 'sha256=<64 hex chars>'")
    return match.group(1).lower(), match.group(2).lower()


def total_parts(size: int, chunk_size: int) -> int:
    """Number of parts needed to carry ``size`` bytes in ``chunk_size`` pieces."""
    if size <= 0:
        return 0
    return -(-size // chunk_size)


def composite_checksum(part_digests: Iterable[bytes]) -> str:
    """Checksum of an upload computed from its parts.

    It is the SHA-256 of the concatenated raw SHA-256 digests of every part, in
    part order. Both sides can compute it cheaply without re-reading the whole
    file, which makes it a good end-to-end integrity check for very large files.
    """
    digest = hashlib.sha256()
    for part_digest in part_digests:
        digest.update(part_digest)
    return format_checksum(digest.hexdigest())


def parse_size(text: str | int) -> int:
    """Parse a human size such as ``8M``, ``512KiB``, ``1.5G`` or ``1048576``.

    Units are binary (``1K == 1024``).
    """
    if isinstance(text, int):
        return text
    match = _SIZE_RE.match(text)
    if match is None:
        raise ValueError(f"invalid size {text!r}")
    number, unit, _ = match.groups()
    return int(float(number) * _SIZE_UNITS[unit.lower()])
