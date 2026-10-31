"""In-process request rate limiting (token bucket per key)."""

from __future__ import annotations

import threading
import time
from collections import OrderedDict
from collections.abc import Callable


class RateLimiter:
    """Token-bucket limiter keyed by caller (owner, or client address).

    Each key may make ``burst`` requests at once and then ``rate`` requests
    per second on average.

    The state lives in the process, so with N server worker processes a caller
    can reach up to N times the limit. That is fine for protecting a server from
    a runaway client; for exact global limits put a limiter in front of the
    service (reverse proxy, API gateway).

    Note that one large upload is many requests (one per part): size the limit
    for ``concurrency × parts per second``, not for "uploads per second".
    """

    def __init__(
        self,
        rate: float,
        burst: int,
        *,
        max_keys: int = 100_000,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if rate <= 0 or burst < 1:
            raise ValueError("rate must be > 0 and burst >= 1")
        self.rate = float(rate)
        self.burst = float(burst)
        self.max_keys = max_keys
        self._clock = clock
        self._buckets: OrderedDict[str, tuple[float, float]] = OrderedDict()
        self._lock = threading.Lock()

    def acquire(self, key: str, cost: float = 1.0) -> float:
        """Take ``cost`` tokens for ``key``.

        Returns 0 when the request may proceed, otherwise the number of seconds
        to wait before retrying.
        """
        now = self._clock()
        with self._lock:
            tokens, last = self._buckets.pop(key, (self.burst, now))
            tokens = min(self.burst, tokens + (now - last) * self.rate)
            if tokens >= cost:
                wait = 0.0
                tokens -= cost
            else:
                wait = (cost - tokens) / self.rate
            self._buckets[key] = (tokens, now)
            while len(self._buckets) > self.max_keys:  # forget the least recently seen
                self._buckets.popitem(last=False)
        return wait
