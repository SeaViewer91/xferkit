"""Shared client plumbing: retries, progress, sync bridging, resume journals."""

from __future__ import annotations

import asyncio
import contextlib
import json
import random
import time
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import IO, Any, TypeVar

import httpx

from ..errors import XferkitError, error_from_response

T = TypeVar("T")

DEFAULT_TIMEOUT = httpx.Timeout(60.0, connect=15.0)
RETRYABLE_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})


@dataclass(frozen=True)
class TransferProgress:
    """Progress of one upload or download."""

    bytes_done: int
    bytes_total: int

    @property
    def fraction(self) -> float:
        return 1.0 if self.bytes_total == 0 else self.bytes_done / self.bytes_total


ProgressCallback = Callable[[TransferProgress], None]


class ProgressTracker:
    def __init__(self, total: int, callback: ProgressCallback | None) -> None:
        self.total = total
        self.done = 0
        self._callback = callback

    def add(self, nbytes: int) -> None:
        if not nbytes:
            return
        self.done += nbytes
        if self._callback is not None:
            self._callback(TransferProgress(self.done, self.total))


@dataclass(frozen=True)
class RetryPolicy:
    """Exponential back-off with jitter.

    Network errors and the statuses in ``retry_statuses`` are retried up to
    ``attempts`` times in total. ``Retry-After`` headers are honoured.
    """

    attempts: int = 6
    backoff: float = 0.5
    max_backoff: float = 30.0
    retry_statuses: frozenset[int] = RETRYABLE_STATUSES

    def delay(self, attempt: int, retry_after: str | None = None) -> float:
        if retry_after:
            seconds = _parse_retry_after(retry_after)
            if seconds is not None:
                return min(seconds, self.max_backoff)
        base = min(self.max_backoff, self.backoff * (2**attempt))
        return base * random.uniform(0.5, 1.0)


def _parse_retry_after(value: str) -> float | None:
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        return max(0.0, parsedate_to_datetime(value).timestamp() - time.time())
    except (TypeError, ValueError):
        return None


def response_error(response: httpx.Response) -> XferkitError:
    """Build the exception matching an error response (body must be read)."""
    try:
        body: Any = response.json()
    except ValueError:
        body = response.text
    return error_from_response(response.status_code, body)


async def send_with_retry(
    policy: RetryPolicy,
    send: Callable[[], Awaitable[httpx.Response]],
    *,
    extra_retry_statuses: frozenset[int] = frozenset(),
) -> httpx.Response:
    """Call ``send`` until it returns a non-retryable response.

    Returns the final response (which may be an error response once the
    attempts are exhausted). Re-raises the last transport error.
    """
    statuses = policy.retry_statuses | extra_retry_statuses
    for attempt in range(policy.attempts):
        last = attempt == policy.attempts - 1
        try:
            response = await send()
        except httpx.TransportError:
            if last:
                raise
            await asyncio.sleep(policy.delay(attempt))
            continue
        if response.status_code in statuses and not last:
            await response.aread()
            await response.aclose()
            await asyncio.sleep(policy.delay(attempt, response.headers.get("retry-after")))
            continue
        return response
    raise AssertionError("unreachable")  # pragma: no cover


def raise_for_error(response: httpx.Response) -> None:
    if response.is_error:
        raise response_error(response)


def run_sync(factory: Callable[[], Coroutine[Any, Any, T]]) -> T:
    """Run a coroutine from synchronous code.

    Raises ``RuntimeError`` inside a running event loop: use the ``Async*``
    classes there instead.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(factory())
    raise RuntimeError(
        "called the synchronous xferkit API from a running event loop; "
        "use AsyncUploader / AsyncDownloader instead"
    )


def make_client(
    client: httpx.AsyncClient | None,
    *,
    headers: dict[str, str] | None,
    timeout: httpx.Timeout | float | None,
    concurrency: int,
) -> tuple[httpx.AsyncClient, bool]:
    """Return ``(client, owned)``; ``owned`` clients are closed by us."""
    if client is not None:
        return client, False
    limits = httpx.Limits(
        max_connections=max(10, concurrency * 2), max_keepalive_connections=max(5, concurrency)
    )
    return (
        httpx.AsyncClient(
            headers=headers,
            timeout=DEFAULT_TIMEOUT if timeout is None else timeout,
            limits=limits,
            follow_redirects=True,
        ),
        True,
    )


class Journal:
    """Append-only resume journal.

    Line 1 is a JSON header describing the transfer; every following line is
    one completed unit of work. Appending a line is cheap and crash-safe
    (a torn last line is ignored on read), unlike rewriting a JSON document
    after every part of a 10 000-part transfer.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self._fh: IO[str] | None = None

    def read(self) -> tuple[dict[str, Any] | None, list[str]]:
        try:
            text = self.path.read_text(encoding="utf-8")
        except (FileNotFoundError, UnicodeDecodeError):
            return None, []
        lines = text.split("\n")
        if not text.endswith("\n"):
            lines = lines[:-1]  # drop a torn final line
        lines = [line for line in lines if line]
        if not lines:
            return None, []
        try:
            header = json.loads(lines[0])
        except ValueError:
            return None, []
        return (header if isinstance(header, dict) else None), lines[1:]

    def start(self, header: dict[str, Any]) -> None:
        self.close()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(header, ensure_ascii=False) + "\n", encoding="utf-8")

    def append(self, line: str) -> None:
        if self._fh is None:
            self._fh = open(self.path, "a", encoding="utf-8")  # noqa: SIM115
        self._fh.write(line + "\n")
        self._fh.flush()

    def close(self) -> None:
        if self._fh is not None:
            with contextlib.suppress(OSError):
                self._fh.close()
            self._fh = None

    def remove(self) -> None:
        self.close()
        with contextlib.suppress(FileNotFoundError):
            self.path.unlink()
