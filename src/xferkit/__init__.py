"""xferkit — chunked, resumable large-file upload/download SDK for Python servers.

Server side (no third-party dependencies)::

    from xferkit import UploadManager
    manager = UploadManager("/data/xferkit")

FastAPI integration (``pip install "xferkit[fastapi]"``)::

    from xferkit.integrations.fastapi import create_upload_router
    app.include_router(create_upload_router(manager))

Client (``pip install "xferkit[client]"``)::

    from xferkit.client import upload_file, download_file
    upload_file("https://host/uploads", "big.tif")
"""

from ._version import __version__
from .errors import (
    ChecksumMismatch,
    Forbidden,
    InsufficientStorage,
    IntegrityError,
    InvalidGrant,
    InvalidPart,
    InvalidRequest,
    PayloadTooLarge,
    QuotaExceeded,
    RangeNotSatisfiable,
    RateLimited,
    RemoteChangedError,
    TooManyUploads,
    TransferError,
    Unauthorized,
    UploadConflict,
    UploadExpired,
    UploadIncomplete,
    UploadNotFound,
    XferkitError,
)
from .grants import Grant, GrantSigner
from .models import CompletedUpload, PartRecord, UploadSession, UploadStatus
from .server import CreateRequest, LocalStorage, RateLimiter, StorageBackend, UploadManager

__all__ = [
    "ChecksumMismatch",
    "CompletedUpload",
    "CreateRequest",
    "Forbidden",
    "Grant",
    "GrantSigner",
    "InsufficientStorage",
    "IntegrityError",
    "InvalidGrant",
    "InvalidPart",
    "InvalidRequest",
    "LocalStorage",
    "PartRecord",
    "PayloadTooLarge",
    "QuotaExceeded",
    "RangeNotSatisfiable",
    "RateLimited",
    "RateLimiter",
    "RemoteChangedError",
    "StorageBackend",
    "TooManyUploads",
    "TransferError",
    "Unauthorized",
    "UploadConflict",
    "UploadExpired",
    "UploadIncomplete",
    "UploadManager",
    "UploadNotFound",
    "UploadSession",
    "UploadStatus",
    "XferkitError",
    "__version__",
]
