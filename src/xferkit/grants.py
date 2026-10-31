"""Signed grants: short-lived, scoped credentials (presigned URLs).

A grant lets someone who has no account in your application perform a limited
set of requests — e.g. a browser uploading straight to the transfer server, an
on-premise worker fetching one result file, or a download link in an e-mail.

Your application (which knows who the user is) *issues* a grant; the transfer
server *verifies* it with the same secret. Nothing is stored server-side.

    signer = GrantSigner(os.environ["XFERKIT_GRANT_SECRET"])

    # Let user "alice" upload files up to 20 GB through /uploads for one hour:
    token = signer.issue(owner="alice", prefix="/uploads", methods="*", ttl=3600,
                         max_size=20 * 1024**3)

    # A download link valid for 10 minutes:
    url = signer.sign_url("https://files.example.com/uploads/abc123/content", ttl=600)

A grant travels either in the ``X-Xferkit-Grant`` header or in the ``xk``
query parameter (the latter makes plain links work).

Token format: ``v1.<base64url(json payload)>.<base64url(HMAC-SHA256)>``.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from .errors import InvalidGrant

GRANT_HEADER = "X-Xferkit-Grant"
GRANT_QUERY = "xk"
_VERSION = "v1"


def _b64encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _b64decode(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def _normalise_methods(methods: str | Iterable[str]) -> frozenset[str]:
    if isinstance(methods, str):
        methods = methods.split(",")
    result = frozenset(m.strip().upper() for m in methods if m.strip())
    if not result:
        raise ValueError("a grant needs at least one method (or '*')")
    if "GET" in result:
        result |= {"HEAD"}
    return result


def _path_matches(path: str, prefix: str) -> bool:
    if prefix in ("", "/"):
        return True
    prefix = prefix.rstrip("/")
    return path == prefix or path.startswith(prefix + "/")


@dataclass(frozen=True)
class Grant:
    """A verified grant."""

    owner: str | None
    prefix: str
    methods: frozenset[str]
    expires_at: int
    max_size: int | None = None

    def allows(self, method: str, path: str) -> bool:
        method_ok = "*" in self.methods or method.upper() in self.methods
        return method_ok and _path_matches(path, self.prefix)


class GrantSigner:
    """Issues and verifies grants with an HMAC secret.

    Args:
        secret: At least 32 bytes of randomness shared by the issuing
            application and the transfer servers. Generate one with
            ``python -c "import secrets; print(secrets.token_urlsafe(32))"``.
        previous_secrets: Older secrets still accepted for verification, for
            rotating the secret without breaking grants already handed out.
        max_ttl: Upper bound on a grant's lifetime, in seconds.
        clock: Time source, injectable for tests.
    """

    def __init__(
        self,
        secret: str | bytes,
        *,
        previous_secrets: Iterable[str | bytes] = (),
        max_ttl: int = 7 * 24 * 3600,
        clock: Callable[[], float] = time.time,
    ) -> None:
        keys = [secret, *previous_secrets]
        self._keys = [k.encode("utf-8") if isinstance(k, str) else bytes(k) for k in keys]
        if any(len(k) < 32 for k in self._keys):
            raise ValueError("grant secrets must be at least 32 bytes long")
        self.max_ttl = max_ttl
        self._clock = clock

    def _mac(self, key: bytes, message: str) -> str:
        return _b64encode(hmac.new(key, message.encode("ascii"), hashlib.sha256).digest())

    def issue(
        self,
        *,
        owner: str | None = None,
        prefix: str = "/",
        methods: str | Iterable[str] = ("GET", "HEAD"),
        ttl: int = 3600,
        max_size: int | None = None,
    ) -> str:
        """Create a grant token.

        Args:
            owner: Principal the requests act as. Uploads created with the grant
                belong to this owner, and only this owner (or grants for it) can
                see them. ``None`` creates anonymous uploads.
            prefix: URL path the grant covers (the path itself and everything
                below it), e.g. ``"/uploads"`` or ``"/uploads/abc123/content"``.
            methods: Allowed HTTP methods, or ``"*"`` for all. ``GET`` implies
                ``HEAD``. The default only allows reading.
            ttl: Lifetime in seconds.
            max_size: Largest upload (bytes) this grant may create.
        """
        if not 0 < ttl <= self.max_ttl:
            raise ValueError(f"ttl must be between 1 and {self.max_ttl} seconds")
        payload: dict[str, object] = {
            "p": prefix or "/",
            "m": ",".join(sorted(_normalise_methods(methods))),
            "e": int(self._clock()) + int(ttl),
        }
        if owner is not None:
            payload["o"] = owner
        if max_size is not None:
            payload["s"] = int(max_size)
        body = _b64encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
        signed = f"{_VERSION}.{body}"
        return f"{signed}.{self._mac(self._keys[0], signed)}"

    def verify(self, token: str, *, method: str | None = None, path: str | None = None) -> Grant:
        """Check a token's signature and expiry, and that it covers the request.

        Raises:
            InvalidGrant: if the token is malformed, forged, expired, or does not
                allow ``method`` on ``path``.
        """
        try:
            version, body, mac = token.split(".")
        except (AttributeError, ValueError):
            raise InvalidGrant("malformed grant") from None
        if version != _VERSION:
            raise InvalidGrant("unsupported grant version")
        signed = f"{version}.{body}"
        if not any(hmac.compare_digest(mac, self._mac(key, signed)) for key in self._keys):
            raise InvalidGrant("grant signature is invalid")
        try:
            payload = json.loads(_b64decode(body))
            grant = Grant(
                owner=None if payload.get("o") is None else str(payload["o"]),
                prefix=str(payload["p"]),
                methods=_normalise_methods(str(payload["m"])),
                expires_at=int(payload["e"]),
                max_size=None if payload.get("s") is None else int(payload["s"]),
            )
        except (ValueError, KeyError, TypeError):
            raise InvalidGrant("malformed grant") from None
        if self._clock() >= grant.expires_at:
            raise InvalidGrant("grant has expired")
        if method is not None and path is not None and not grant.allows(method, path):
            raise InvalidGrant(f"grant does not allow {method.upper()} {path}")
        return grant

    def sign_url(
        self,
        url: str,
        *,
        owner: str | None = None,
        methods: str | Iterable[str] = ("GET", "HEAD"),
        ttl: int = 3600,
        prefix: str | None = None,
        max_size: int | None = None,
    ) -> str:
        """Return ``url`` with a grant for it in the ``xk`` query parameter.

        By default the grant covers exactly the URL's path, read-only.
        """
        parts = urlsplit(url)
        token = self.issue(
            owner=owner,
            prefix=parts.path if prefix is None else prefix,
            methods=methods,
            ttl=ttl,
            max_size=max_size,
        )
        query = [
            (k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k != GRANT_QUERY
        ]
        query.append((GRANT_QUERY, token))
        return urlunsplit(parts._replace(query=urlencode(query)))
