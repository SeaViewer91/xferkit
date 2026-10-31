"""HTTP client for uploads and downloads (requires ``pip install "xferkit[client]"``)."""

from ._common import ProgressCallback, RetryPolicy, TransferProgress
from .download import AsyncDownloader, Downloader, DownloadResult, download_file
from .upload import (
    AsyncUploader,
    BatchProgress,
    BatchResult,
    BatchUploadError,
    FailedUpload,
    Uploader,
    UploadItem,
    UploadResult,
    upload_file,
    upload_folder,
)

__all__ = [
    "AsyncDownloader",
    "AsyncUploader",
    "BatchProgress",
    "BatchResult",
    "BatchUploadError",
    "DownloadResult",
    "Downloader",
    "FailedUpload",
    "ProgressCallback",
    "RetryPolicy",
    "TransferProgress",
    "UploadItem",
    "UploadResult",
    "Uploader",
    "download_file",
    "upload_file",
    "upload_folder",
]
