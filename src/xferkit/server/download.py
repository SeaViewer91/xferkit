"""Framework-agnostic helpers for serving files with HTTP range requests.

:func:`plan_file_response` looks at a file and the request headers and decides
the status code, the response headers and which bytes to send. Any framework
can then stream those bytes with :func:`iter_file_range`. The FastAPI helper
:func:`xferkit.integrations.fastapi.range_file_response` is a thin wrapper.

Supported: single byte ranges (``bytes=a-b``, ``bytes=a-``, ``bytes=-n``),
``If-Range`` (ETag or date), ``If-None-Match`` (304), ``HEAD``. Multi-range
requests are answered with the full file (allowed by RFC 9110).
"""

from __future__ import annotations

import mimetypes
import os
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from email.utils import formatdate, parsedate_to_datetime
from pathlib import Path
from urllib.parse import quote

from ..errors import RangeNotSatisfiable
from ..protocol import MiB


@dataclass(frozen=True)
class ByteRange:
    """An inclusive byte range ``start..end``."""

    start: int
    end: int

    @property
    def length(self) -> int:
        return self.end - self.start + 1

    def content_range(self, total: int) -> str:
        return f"bytes {self.start}-{self.end}/{total}"


def parse_range_header(header: str | None, size: int) -> ByteRange | None:
    """Parse a ``Range`` header against a resource of ``size`` bytes.

    Returns ``None`` when the whole resource should be sent (no header, an
    unsupported unit, a multi-range request or a malformed value — all of which
    a server may ignore). Raises :class:`RangeNotSatisfiable` for a well-formed
    range that lies outside the resource.
    """
    if not header:
        return None
    unit, _, spec = header.strip().partition("=")
    if unit.strip().lower() != "bytes" or not spec or "," in spec:
        return None
    first, dash, last = spec.strip().partition("-")
    if not dash:
        return None
    first, last = first.strip(), last.strip()
    try:
        if first == "":  # suffix range: last N bytes
            if not last.isdigit():
                return None
            suffix = int(last)
            if suffix == 0 or size == 0:
                raise RangeNotSatisfiable(f"range {header!r} not satisfiable")
            return ByteRange(max(0, size - suffix), size - 1)
        if not first.isdigit() or (last and not last.isdigit()):
            return None
        start = int(first)
        end = int(last) if last else size - 1
    except ValueError:
        return None
    if last and end < start:
        return None
    if start >= size:
        raise RangeNotSatisfiable(f"range {header!r} not satisfiable")
    return ByteRange(start, min(end, size - 1))


def make_etag(stat: os.stat_result) -> str:
    """Strong ETag derived from inode, size and modification time."""
    return f'"{stat.st_ino:x}-{stat.st_size:x}-{stat.st_mtime_ns:x}"'


def content_disposition(filename: str, disposition: str = "attachment") -> str:
    """``Content-Disposition`` value that survives non-ASCII names (RFC 6266/5987)."""
    fallback = filename.encode("ascii", "replace").decode("ascii")
    fallback = "".join("_" if ch in '"\\' or ord(ch) < 32 else ch for ch in fallback) or "file"
    value = f'{disposition}; filename="{fallback}"'
    if fallback != filename:
        value += f"; filename*=UTF-8''{quote(filename, safe='')}"
    return value


def _if_range_matches(value: str, etag: str, mtime: float) -> bool:
    value = value.strip()
    if value.startswith(('"', "W/")):
        return value == etag  # strong comparison; weak tags never match
    try:
        return int(parsedate_to_datetime(value).timestamp()) == int(mtime)
    except (TypeError, ValueError):
        return False


def _etag_listed(value: str, etag: str) -> bool:
    candidates = [tag.strip() for tag in value.split(",")]
    weak = etag[2:] if etag.startswith("W/") else etag
    return "*" in candidates or any(
        (tag[2:] if tag.startswith("W/") else tag) == weak for tag in candidates
    )


@dataclass(frozen=True)
class FilePlan:
    """What to answer for a file request."""

    status: int
    headers: dict[str, str]
    path: Path
    start: int = 0
    length: int = 0
    send_body: bool = True
    etag: str = ""
    size: int = 0


HeaderGetter = Mapping[str, str] | Callable[[str], str | None]


def _header_lookup(headers: HeaderGetter | None) -> Callable[[str], str | None]:
    if headers is None:
        return lambda name: None
    if callable(headers) and not isinstance(headers, Mapping):
        return headers
    lowered = {str(key).lower(): value for key, value in headers.items()}
    return lambda name: lowered.get(name.lower())


def plan_file_response(
    path: str | os.PathLike[str],
    *,
    request_headers: HeaderGetter | None = None,
    method: str = "GET",
    filename: str | None = None,
    media_type: str | None = None,
    disposition: str | None = "attachment",
) -> FilePlan:
    """Decide how to answer a ``GET``/``HEAD`` request for ``path``.

    Args:
        path: File to serve. ``FileNotFoundError`` propagates.
        request_headers: The request headers (any mapping or a getter function).
        method: ``"GET"`` or ``"HEAD"``.
        filename: Name advertised in ``Content-Disposition`` (default: the
            file's own name).
        media_type: ``Content-Type`` (default: guessed from the name).
        disposition: ``"attachment"``, ``"inline"`` or ``None`` for no header.
    """
    path = Path(path)
    stat = path.stat()
    if not path.is_file():
        raise FileNotFoundError(str(path))
    get = _header_lookup(request_headers)
    size = stat.st_size
    etag = make_etag(stat)
    name = filename or path.name
    headers = {
        "Accept-Ranges": "bytes",
        "ETag": etag,
        "Last-Modified": formatdate(stat.st_mtime, usegmt=True),
    }
    send_body = method.upper() != "HEAD"

    if_none_match = get("if-none-match")
    if if_none_match and _etag_listed(if_none_match, etag):
        return FilePlan(304, headers, path, send_body=False, etag=etag, size=size)

    headers["Content-Type"] = (
        media_type or mimetypes.guess_type(name)[0] or "application/octet-stream"
    )
    if disposition:
        headers["Content-Disposition"] = content_disposition(name, disposition)

    byte_range: ByteRange | None = None
    range_header = get("range")
    if range_header:
        if_range = get("if-range")
        if not if_range or _if_range_matches(if_range, etag, stat.st_mtime):
            try:
                byte_range = parse_range_header(range_header, size)
            except RangeNotSatisfiable:
                headers["Content-Range"] = f"bytes */{size}"
                headers["Content-Length"] = "0"
                return FilePlan(416, headers, path, send_body=False, etag=etag, size=size)

    if byte_range is None:
        headers["Content-Length"] = str(size)
        return FilePlan(200, headers, path, 0, size, send_body, etag, size)
    headers["Content-Range"] = byte_range.content_range(size)
    headers["Content-Length"] = str(byte_range.length)
    return FilePlan(206, headers, path, byte_range.start, byte_range.length, send_body, etag, size)


def iter_file_range(
    path: str | os.PathLike[str], start: int, length: int, block_size: int = MiB
) -> Iterator[bytes]:
    """Yield ``length`` bytes of ``path`` starting at ``start``."""
    remaining = length
    with open(path, "rb") as fh:
        fh.seek(start)
        while remaining > 0:
            block = fh.read(min(block_size, remaining))
            if not block:
                break
            remaining -= len(block)
            yield block
