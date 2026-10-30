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
    IntegrityError,
    InvalidPart,
    InvalidRequest,
    PayloadTooLarge,
    RangeNotSatisfiable,
    RemoteChangedError,
    TransferError,
    UploadConflict,
    UploadExpired,
    UploadIncomplete,
    UploadNotFound,
    XferkitError,
)
from .models import CompletedUpload, PartRecord, UploadSession, UploadStatus
from .server import LocalStorage, StorageBackend, UploadManager

__all__ = [
    "ChecksumMismatch",
    "CompletedUpload",
    "IntegrityError",
    "InvalidPart",
    "InvalidRequest",
    "LocalStorage",
    "PartRecord",
    "PayloadTooLarge",
    "RangeNotSatisfiable",
    "RemoteChangedError",
    "StorageBackend",
    "TransferError",
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
