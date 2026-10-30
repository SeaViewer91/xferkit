"""FastAPI / Starlette integration.

Mount the upload protocol::

    from fastapi import FastAPI
    from xferkit import UploadManager
    from xferkit.integrations.fastapi import create_upload_router

    manager = UploadManager("/data/xferkit")
    app = FastAPI()
    app.include_router(create_upload_router(manager, prefix="/uploads"))

Serve any file with resumable, parallel-friendly range support::

    from xferkit.integrations.fastapi import range_file_response

    @app.head("/files/{name}", include_in_schema=False)
    @app.get("/files/{name}")
    async def get_file(name: str, request: Request):
        return range_file_response(request, safe_path_for(name))

Request bodies are streamed straight to disk; a part is never held in memory
in full. Blocking file I/O runs in Starlette's thread pool.
"""

import inspect
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.params import Depends
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from ..errors import InvalidPart, XferkitError
from ..models import CompletedUpload
from ..protocol import CHECKSUM_HEADER, PROTOCOL_HEADER, PROTOCOL_VERSION, MiB
from ..server.download import iter_file_range, plan_file_response
from ..server.manager import UploadManager

__all__ = [
    "CreateUploadRequest",
    "create_upload_router",
    "error_response",
    "range_file_response",
]

CompleteCallback = Callable[[CompletedUpload, Request], Any]

_BASE_HEADERS = {PROTOCOL_HEADER: PROTOCOL_VERSION}


class CreateUploadRequest(BaseModel):
    """Body of ``POST {prefix}``."""

    filename: str = Field(min_length=1, max_length=1024, description="Original file name")
    size: int = Field(ge=0, description="Exact file size in bytes")
    chunk_size: int | None = Field(
        default=None, gt=0, description="Requested part size; omit to let the server choose"
    )
    checksum: str | None = Field(
        default=None, description="Optional whole-file checksum, 'sha256=<hex>'"
    )
    metadata: dict[str, str] = Field(default_factory=dict)
    client_token: str | None = Field(
        default=None,
        pattern=r"^[A-Za-z0-9_-]{8,128}$",
        description=(
            "Random id chosen by the client before sending. Re-sending the same token "
            "returns the existing upload (200) instead of creating another one (201)."
        ),
    )


def error_response(exc: XferkitError) -> JSONResponse:
    """Turn an :class:`XferkitError` into the protocol's JSON error response."""
    return JSONResponse(exc.to_dict(), status_code=exc.status_code, headers=_BASE_HEADERS)


def _json(data: Any, status_code: int = 200, headers: dict[str, str] | None = None) -> JSONResponse:
    return JSONResponse(data, status_code=status_code, headers={**_BASE_HEADERS, **(headers or {})})


def create_upload_router(
    manager: UploadManager,
    *,
    prefix: str = "/uploads",
    tags: Sequence[str] = ("xferkit",),
    dependencies: Sequence[Depends] | None = None,
    on_complete: CompleteCallback | None = None,
    expose_downloads: bool = False,
    write_buffer_size: int = MiB,
) -> APIRouter:
    """Build an :class:`APIRouter` that speaks the xferkit upload protocol.

    Args:
        manager: The :class:`UploadManager` that stores the uploads.
        prefix: URL prefix of the protocol endpoints.
        tags: OpenAPI tags.
        dependencies: Router-level dependencies, e.g. ``[Depends(require_user)]``
            to protect every endpoint.
        on_complete: Called once per finished upload with
            ``(completed, request)``; may be sync or ``async``. Runs after
            ``manager.on_complete``. Use it for request-scoped work (who
            uploaded it, DB rows, background jobs).
        expose_downloads: Also add ``GET/HEAD {prefix}/{upload_id}/content``
            serving completed uploads with range support. Off by default:
            anyone who knows an upload id could read the file.
        write_buffer_size: Bytes buffered before each hand-off to the I/O thread.
    """
    router = APIRouter(prefix=prefix, tags=list(tags), dependencies=list(dependencies or []))
    root = "" if prefix else "/"

    @router.post(root, status_code=201, summary="Create an upload session")
    async def create_upload(body: CreateUploadRequest, request: Request) -> Response:
        try:
            state, created = await run_in_threadpool(
                lambda: manager.open_upload(
                    body.filename,
                    body.size,
                    chunk_size=body.chunk_size,
                    checksum=body.checksum,
                    metadata=body.metadata,
                    client_token=body.client_token,
                )
            )
        except XferkitError as exc:
            return error_response(exc)
        location = str(request.url_for(f"{_name(prefix)}_status", upload_id=state["upload_id"]))
        return _json(state, 201 if created else 200, {"Location": location})

    @router.get("/{upload_id}", name=f"{_name(prefix)}_status", summary="Upload status")
    async def get_status(upload_id: str) -> Response:
        try:
            status = await run_in_threadpool(manager.describe, upload_id)
        except XferkitError as exc:
            return error_response(exc)
        return _json(status)

    @router.put("/{upload_id}/parts/{index}", summary="Upload one part (raw request body)")
    async def put_part(upload_id: str, index: int, request: Request) -> Response:
        try:
            session = await run_in_threadpool(manager.get_session, upload_id)
            expected = session.part_size(index)
            declared = request.headers.get("content-length")
            if declared is not None and declared.strip() != str(expected):
                raise InvalidPart(
                    f"part {index} must be exactly {expected} bytes, Content-Length is {declared}",
                    details={"part": index, "expected_size": expected},
                )
            sink = await run_in_threadpool(manager.open_part, upload_id, index)
        except XferkitError as exc:
            return error_response(exc)

        try:
            buffer = bytearray()
            async for block in request.stream():
                buffer += block
                if len(buffer) >= write_buffer_size:
                    await run_in_threadpool(sink.write, bytes(buffer))
                    buffer.clear()
            if buffer:
                await run_in_threadpool(sink.write, bytes(buffer))
            record = await run_in_threadpool(sink.commit, request.headers.get(CHECKSUM_HEADER))
        except XferkitError as exc:
            await run_in_threadpool(sink.abort)
            return error_response(exc)
        except BaseException:  # client disconnect, cancellation, ...
            await run_in_threadpool(sink.abort)
            raise
        return _json(record.to_dict())

    @router.post("/{upload_id}/complete", summary="Assemble and verify the upload")
    async def complete_upload(upload_id: str, request: Request) -> Response:
        try:
            completed, created = await run_in_threadpool(manager.complete_upload, upload_id)
        except XferkitError as exc:
            return error_response(exc)
        if created and on_complete is not None:
            result = on_complete(completed, request)
            if inspect.isawaitable(result):
                await result
        return _json(completed.to_dict())

    @router.delete("/{upload_id}", status_code=204, summary="Abort the upload")
    async def abort_upload(upload_id: str) -> Response:
        try:
            await run_in_threadpool(manager.abort, upload_id)
        except XferkitError as exc:
            return error_response(exc)
        return Response(status_code=204, headers=_BASE_HEADERS)

    if expose_downloads:

        @router.head("/{upload_id}/content", include_in_schema=False)
        @router.get("/{upload_id}/content", summary="Download a completed upload (supports Range)")
        async def download_completed(upload_id: str, request: Request) -> Response:
            try:
                completed = await run_in_threadpool(manager.get_completed, upload_id)
            except XferkitError as exc:
                return error_response(exc)
            return range_file_response(request, completed.path, filename=completed.filename)

    return router


def _name(prefix: str) -> str:
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in prefix).strip("_")
    return f"xferkit_{cleaned}" if cleaned else "xferkit"


def range_file_response(
    request: Request,
    path: str | Path,
    *,
    filename: str | None = None,
    media_type: str | None = None,
    disposition: str | None = "attachment",
    block_size: int = MiB,
    headers: dict[str, str] | None = None,
) -> Response:
    """Serve ``path`` with ``Range``/``If-Range``/``ETag`` support.

    Answers ``200``, ``206``, ``304``, ``416`` or ``404`` (missing file).
    Works for ``GET`` and ``HEAD`` (register the route for both methods).
    """
    try:
        plan = plan_file_response(
            path,
            request_headers=request.headers,
            method=request.method,
            filename=filename,
            media_type=media_type,
            disposition=disposition,
        )
    except (FileNotFoundError, NotADirectoryError):
        raise HTTPException(status_code=404, detail="file not found") from None
    response_headers = {**plan.headers, **(headers or {})}
    if not plan.send_body or plan.length == 0:
        return Response(status_code=plan.status, headers=response_headers)
    return StreamingResponse(
        iter_file_range(plan.path, plan.start, plan.length, block_size),
        status_code=plan.status,
        headers=response_headers,
    )
