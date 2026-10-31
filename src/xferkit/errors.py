"""Exception hierarchy shared by the server core, the integrations and the client.

Every server-side error carries an HTTP ``status_code`` and a stable ``code``
string, so integrations can turn it into a JSON error body and the client can
turn that body back into the same exception class.
"""

from __future__ import annotations

from typing import Any


class XferkitError(Exception):
    """Base class for all xferkit errors."""

    status_code: int = 500
    code: str = "internal_error"

    def __init__(
        self,
        message: str = "",
        *,
        details: dict[str, Any] | None = None,
        status_code: int | None = None,
    ) -> None:
        super().__init__(message or self.code)
        self.message = message or self.code
        self.details: dict[str, Any] = dict(details or {})
        if status_code is not None:
            self.status_code = status_code

    def to_dict(self) -> dict[str, Any]:
        body: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.details:
            body["details"] = self.details
        return {"error": body}


class InvalidRequest(XferkitError):
    """The request is malformed or violates a server limit."""

    status_code = 400
    code = "invalid_request"


class InvalidPart(InvalidRequest):
    """A part index or part body does not match the upload session."""

    code = "invalid_part"


class PayloadTooLarge(XferkitError):
    """The upload or the part exceeds the configured size limits."""

    status_code = 413
    code = "payload_too_large"


class ChecksumMismatch(XferkitError):
    """Received data does not match the checksum declared by the sender."""

    status_code = 422
    code = "checksum_mismatch"


class UploadNotFound(XferkitError):
    """No upload session exists with this id."""

    status_code = 404
    code = "upload_not_found"


class UploadExpired(UploadNotFound):
    """The upload session exists but has passed its expiry time."""

    status_code = 410
    code = "upload_expired"


class UploadConflict(XferkitError):
    """The operation conflicts with the current state of the session."""

    status_code = 409
    code = "upload_conflict"


class UploadIncomplete(UploadConflict):
    """``complete`` was called while some parts are still missing.

    ``details["missing_parts"]`` lists the missing part indices.
    """

    code = "upload_incomplete"


class Unauthorized(XferkitError):
    """The request carries no (valid) credentials."""

    status_code = 401
    code = "unauthorized"


class InvalidGrant(Unauthorized):
    """A signed grant is malformed, forged, expired or does not cover the request."""

    code = "invalid_grant"


class Forbidden(XferkitError):
    """The caller is known but not allowed to do this."""

    status_code = 403
    code = "forbidden"


class QuotaExceeded(Forbidden):
    """The caller's quota does not allow this upload."""

    code = "quota_exceeded"


class RateLimited(XferkitError):
    """Too many requests; ``details["retry_after"]`` says how long to wait."""

    status_code = 429
    code = "rate_limited"


class TooManyUploads(RateLimited):
    """Too many uploads in progress; finish or abort some first."""

    code = "too_many_uploads"


class InsufficientStorage(XferkitError):
    """The server does not have enough free disk space for this upload."""

    status_code = 507
    code = "insufficient_storage"


class RangeNotSatisfiable(XferkitError):
    """The requested byte range lies outside the file."""

    status_code = 416
    code = "range_not_satisfiable"


# --------------------------------------------------------------------------- client side


class TransferError(XferkitError):
    """The server answered with an unexpected status or an unknown error."""

    code = "transfer_error"


class IntegrityError(XferkitError):
    """Data verified locally does not match what the server reports."""

    code = "integrity_error"


class RemoteChangedError(XferkitError):
    """The remote file changed while it was being downloaded."""

    status_code = 412
    code = "remote_changed"


_ERRORS_BY_CODE: dict[str, type[XferkitError]] = {
    cls.code: cls
    for cls in (
        InvalidRequest,
        InvalidPart,
        PayloadTooLarge,
        ChecksumMismatch,
        UploadNotFound,
        UploadExpired,
        UploadConflict,
        UploadIncomplete,
        RangeNotSatisfiable,
        Unauthorized,
        InvalidGrant,
        Forbidden,
        QuotaExceeded,
        RateLimited,
        TooManyUploads,
        InsufficientStorage,
    )
}


def error_from_response(status_code: int, body: Any) -> XferkitError:
    """Rebuild the matching exception from an error response body."""
    error = body.get("error") if isinstance(body, dict) else None
    if isinstance(error, dict):
        cls = _ERRORS_BY_CODE.get(str(error.get("code")), TransferError)
        return cls(
            str(error.get("message") or f"HTTP {status_code}"),
            details=error.get("details") if isinstance(error.get("details"), dict) else None,
            status_code=status_code,
        )
    text = body if isinstance(body, str) else ""
    message = f"unexpected HTTP {status_code}" + (f": {text[:200]}" if text else "")
    return TransferError(message, status_code=status_code)
