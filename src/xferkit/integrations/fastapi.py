"""FastAPI / Starlette integration.

Mount the upload protocol::

    from fastapi import FastAPI
    from xferkit import UploadManager
    from xferkit.integrations.fastapi import create_upload_router

    manager = UploadManager("/data/xferkit")
    app = FastAPI()
    app.include_router(create_upload_router(manager, prefix="/uploads"))

Authentication (who is calling) is pluggable: pass ``principal=`` to identify
callers with your own auth (session cookie, JWT, API key, ...) and/or
``grants=`` to accept signed grants (presigned URLs). Uploads created by a
caller belong to it and are invisible to everybody else.

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
import math
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import APIRouter, FastAPI, HTTPException, Request
from fastapi.params import Depends
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from ..errors import (
    Forbidden,
    InvalidGrant,
    InvalidPart,
    PayloadTooLarge,
    RateLimited,
    Unauthorized,
    UploadNotFound,
    XferkitError,
)
from ..grants import GRANT_HEADER, GRANT_QUERY, Grant, GrantSigner
from ..models import CompletedUpload
from ..protocol import CHECKSUM_HEADER, PROTOCOL_HEADER, PROTOCOL_VERSION, MiB
from ..server.download import iter_file_range, plan_file_response
from ..server.manager import UploadManager
from ..server.ratelimit import RateLimiter

__all__ = [
    "CORS_ALLOW_HEADERS",
    "CORS_EXPOSE_HEADERS",
    "AccessContext",
    "BatchCreateRequest",
    "BatchStatusRequest",
    "Caller",
    "CreateUploadRequest",
    "add_cors",
    "create_upload_router",
    "error_response",
    "mount_browser_client",
    "range_file_response",
    "require_grant",
]

CompleteCallback = Callable[[CompletedUpload, Request], Any]
PrincipalResolver = Callable[[Request], "Awaitable[str | None] | str | None"]

#: Request headers a browser client sends (for ``Access-Control-Allow-Headers``).
CORS_ALLOW_HEADERS = [
    "Authorization",
    "Content-Type",
    "If-Range",
    "Range",
    CHECKSUM_HEADER,
    GRANT_HEADER,
]
#: Response headers a browser client must be able to read.
CORS_EXPOSE_HEADERS = [
    "Accept-Ranges",
    "Content-Disposition",
    "Content-Length",
    "Content-Range",
    "ETag",
    "Last-Modified",
    "Location",
    "Retry-After",
    PROTOCOL_HEADER,
]

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
    auto_complete: bool = Field(
        default=False,
        description=(
            "Complete the upload as soon as its last part arrives (the part response then "
            "contains 'completed'), saving the separate complete request. For small files."
        ),
    )

    def manager_kwargs(self) -> dict[str, Any]:
        return {
            "filename": self.filename,
            "size": self.size,
            "chunk_size": self.chunk_size,
            "checksum": self.checksum,
            "metadata": self.metadata,
            "client_token": self.client_token,
            "auto_complete": self.auto_complete,
        }


class BatchCreateRequest(BaseModel):
    """Body of ``POST {prefix}/batch``."""

    files: list[CreateUploadRequest] = Field(min_length=1, max_length=1000)


class BatchStatusRequest(BaseModel):
    """Body of ``POST {prefix}/batch/status``."""

    upload_ids: list[str] = Field(min_length=1, max_length=1000)


def _batch_item(outcome: Any) -> dict[str, Any]:
    if isinstance(outcome, XferkitError):
        return {**outcome.to_dict(), "status": outcome.status_code}
    if isinstance(outcome, tuple):
        state, created = outcome
        return {**state, "created": created}
    return outcome


@dataclass(frozen=True)
class Caller:
    """Who is making a request (also available as ``request.state.xferkit_caller``)."""

    owner: str | None
    grant: Grant | None = None

    @property
    def authenticated(self) -> bool:
        return self.owner is not None or self.grant is not None


@dataclass(frozen=True)
class AccessContext:
    """Passed to the ``authorize`` hook."""

    caller: Caller
    action: str  # "create", "status", "upload_part", "complete", "abort", "download"
    upload_id: str | None = None
    create: CreateUploadRequest | None = None


Authorizer = Callable[[Request, AccessContext], "Awaitable[bool | None] | bool | None"]


def error_response(exc: XferkitError) -> JSONResponse:
    """Turn an :class:`XferkitError` into the protocol's JSON error response."""
    headers = dict(_BASE_HEADERS)
    retry_after = exc.details.get("retry_after")
    if retry_after is not None:
        headers["Retry-After"] = str(math.ceil(float(retry_after)))
    if isinstance(exc, Unauthorized):
        headers["WWW-Authenticate"] = "Xferkit-Grant"
    return JSONResponse(exc.to_dict(), status_code=exc.status_code, headers=headers)


def _json(data: Any, status_code: int = 200, headers: dict[str, str] | None = None) -> JSONResponse:
    return JSONResponse(data, status_code=status_code, headers={**_BASE_HEADERS, **(headers or {})})


async def _maybe_await(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


def _grant_names_upload(grant: Grant | None, upload_id: str) -> bool:
    """A grant whose path names one specific upload is a capability for it."""
    return grant is not None and upload_id in grant.prefix.strip("/").split("/")


def create_upload_router(
    manager: UploadManager,
    *,
    prefix: str = "/uploads",
    tags: Sequence[str] = ("xferkit",),
    dependencies: Sequence[Depends] | None = None,
    principal: PrincipalResolver | None = None,
    grants: GrantSigner | None = None,
    require_auth: bool = False,
    authorize: Authorizer | None = None,
    rate_limit: RateLimiter | None = None,
    on_complete: CompleteCallback | None = None,
    expose_downloads: bool = False,
    write_buffer_size: int = MiB,
) -> APIRouter:
    """Build an :class:`APIRouter` that speaks the xferkit upload protocol.

    Args:
        manager: The :class:`UploadManager` that stores the uploads.
        prefix: URL prefix of the protocol endpoints.
        tags: OpenAPI tags.
        dependencies: Extra router-level FastAPI dependencies.
        principal: Identifies the caller with your own authentication: called
            with the request, returns an owner id (``str``) or ``None`` for an
            anonymous caller; may be ``async`` and may raise ``HTTPException``.
            Uploads belong to the caller that created them; other callers get
            ``404`` for them.
        grants: Accept signed grants (``X-Xferkit-Grant`` header or ``xk``
            query parameter) issued with this :class:`GrantSigner`. A valid
            grant authenticates the request as the grant's owner.
        require_auth: Reject requests that are neither identified by
            ``principal`` nor carry a valid grant (``401``).
        authorize: Extra policy hook ``(request, AccessContext) -> bool``;
            returning ``False`` answers ``403``.
        rate_limit: Per-caller request rate limit (``429`` + ``Retry-After``).
        on_complete: Called once per finished upload with
            ``(completed, request)``; may be sync or ``async``. Runs after
            ``manager.on_complete``. ``request.state.xferkit_caller`` tells who
            uploaded it.
        expose_downloads: Also add ``GET/HEAD {prefix}/{upload_id}/content``
            serving completed uploads with range support (subject to the same
            ownership rules, or to a grant naming the upload).
        write_buffer_size: Bytes buffered before each hand-off to the I/O thread.
    """
    router = APIRouter(prefix=prefix, tags=list(tags), dependencies=list(dependencies or []))
    root = "" if prefix else "/"

    async def resolve_caller(request: Request) -> Caller:
        token = request.headers.get(GRANT_HEADER) or request.query_params.get(GRANT_QUERY)
        if token:
            if grants is None:
                raise InvalidGrant("this server does not accept grants")
            grant = grants.verify(token, method=request.method, path=request.url.path)
            return Caller(owner=grant.owner, grant=grant)
        owner = await _maybe_await(principal(request)) if principal is not None else None
        caller = Caller(owner=owner)
        if require_auth and not caller.authenticated:
            raise Unauthorized("authentication required")
        return caller

    async def guard(
        request: Request,
        action: str,
        *,
        upload_id: str | None = None,
        create: CreateUploadRequest | None = None,
        check_owner: bool = True,
        run_authorize: bool = True,
    ) -> Caller:
        caller = await resolve_caller(request)
        request.state.xferkit_caller = caller
        if rate_limit is not None:
            key = caller.owner or f"ip:{request.client.host if request.client else '?'}"
            wait = rate_limit.acquire(key)
            if wait > 0:
                raise RateLimited("too many requests", details={"retry_after": wait})
        if (
            upload_id is not None
            and check_owner
            and not _grant_names_upload(caller.grant, upload_id)
        ):
            await run_in_threadpool(manager.ensure_access, upload_id, caller.owner)
        if authorize is not None and run_authorize:
            context = AccessContext(caller, action, upload_id, create)
            if await _maybe_await(authorize(request, context)) is False:
                raise Forbidden(f"not allowed to {action.replace('_', ' ')}")
        return caller

    @router.post(root, status_code=201, summary="Create an upload session")
    async def create_upload(body: CreateUploadRequest, request: Request) -> Response:
        try:
            caller = await guard(request, "create", create=body)
            limit = caller.grant.max_size if caller.grant is not None else None
            if limit is not None and body.size > limit:
                raise PayloadTooLarge(
                    "file is larger than this grant allows", details={"max_size": limit}
                )
            state, created = await run_in_threadpool(
                lambda: manager.open_upload(**body.manager_kwargs(), owner=caller.owner)
            )
        except XferkitError as exc:
            return error_response(exc)
        location = str(request.url_for(f"{_name(prefix)}_status", upload_id=state["upload_id"]))
        return _json(state, 201 if created else 200, {"Location": location})

    @router.post("/batch", summary="Create (or find, by client_token) many upload sessions")
    async def create_batch(body: BatchCreateRequest, request: Request) -> Response:
        try:
            caller = await guard(request, "create", run_authorize=False)
        except XferkitError as exc:
            return error_response(exc)
        limit = caller.grant.max_size if caller.grant is not None else None
        results: list[Any] = [None] * len(body.files)
        accepted: list[tuple[int, dict[str, Any]]] = []
        for i, item in enumerate(body.files):
            if limit is not None and item.size > limit:
                results[i] = PayloadTooLarge(
                    "file is larger than this grant allows", details={"max_size": limit}
                )
                continue
            if authorize is not None:
                context = AccessContext(caller, "create", None, item)
                if await _maybe_await(authorize(request, context)) is False:
                    results[i] = Forbidden("not allowed to create")
                    continue
            accepted.append((i, item.manager_kwargs()))
        try:
            outcomes = await run_in_threadpool(
                lambda: manager.open_uploads([kw for _, kw in accepted], owner=caller.owner)
            )
        except XferkitError as exc:
            return error_response(exc)
        for (i, _), outcome in zip(accepted, outcomes, strict=True):
            results[i] = outcome
        return _json({"uploads": [_batch_item(r) for r in results]})

    @router.post("/batch/status", summary="Status of many uploads")
    async def batch_status(body: BatchStatusRequest, request: Request) -> Response:
        try:
            caller = await guard(request, "status")
            outcomes = await run_in_threadpool(
                lambda: manager.describe_many(body.upload_ids, caller=caller.owner)
            )
        except XferkitError as exc:
            return error_response(exc)
        return _json({"uploads": [_batch_item(r) for r in outcomes]})

    @router.get("/{upload_id}", name=f"{_name(prefix)}_status", summary="Upload status")
    async def get_status(upload_id: str, request: Request) -> Response:
        try:
            await guard(request, "status", upload_id=upload_id)
            status = await run_in_threadpool(manager.describe, upload_id)
        except XferkitError as exc:
            return error_response(exc)
        return _json(status)

    @router.put("/{upload_id}/parts/{index}", summary="Upload one part (raw request body)")
    async def put_part(upload_id: str, index: int, request: Request) -> Response:
        try:
            caller = await guard(request, "upload_part", upload_id=upload_id, check_owner=False)
            session = await run_in_threadpool(manager.get_session, upload_id)
            if (
                session.owner is not None
                and session.owner != caller.owner
                and not _grant_names_upload(caller.grant, upload_id)
            ):
                raise UploadNotFound(f"upload {upload_id!r} not found")
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
        body = record.to_dict()
        if session.auto_complete:
            try:
                outcome = await run_in_threadpool(manager.complete_if_ready, upload_id)
            except XferkitError as exc:  # the part is stored; report why completion failed
                body["completion_error"] = exc.to_dict()["error"]
            else:
                if outcome is not None:
                    completed, created = outcome
                    if created and on_complete is not None:
                        await _maybe_await(on_complete(completed, request))
                    body["completed"] = completed.to_dict()
        return _json(body)

    @router.post("/{upload_id}/complete", summary="Assemble and verify the upload")
    async def complete_upload(upload_id: str, request: Request) -> Response:
        try:
            await guard(request, "complete", upload_id=upload_id)
            completed, created = await run_in_threadpool(manager.complete_upload, upload_id)
        except XferkitError as exc:
            return error_response(exc)
        if created and on_complete is not None:
            await _maybe_await(on_complete(completed, request))
        return _json(completed.to_dict())

    @router.delete("/{upload_id}", status_code=204, summary="Abort the upload")
    async def abort_upload(upload_id: str, request: Request) -> Response:
        try:
            await guard(request, "abort", upload_id=upload_id)
            await run_in_threadpool(manager.abort, upload_id)
        except XferkitError as exc:
            return error_response(exc)
        return Response(status_code=204, headers=_BASE_HEADERS)

    if expose_downloads:

        @router.head("/{upload_id}/content", include_in_schema=False)
        @router.get("/{upload_id}/content", summary="Download a completed upload (supports Range)")
        async def download_completed(upload_id: str, request: Request) -> Response:
            try:
                await guard(request, "download", upload_id=upload_id)
                completed = await run_in_threadpool(manager.get_completed, upload_id)
                url = await run_in_threadpool(manager.storage.download_url, completed)
            except XferkitError as exc:
                return error_response(exc)
            if url is not None:  # e.g. a presigned object-storage URL
                return Response(
                    status_code=307,
                    headers={**_BASE_HEADERS, "Location": url, "Cache-Control": "no-store"},
                )
            return range_file_response(request, completed.path, filename=completed.filename)

    return router


def _name(prefix: str) -> str:
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in prefix).strip("_")
    return f"xferkit_{cleaned}" if cleaned else "xferkit"


def add_cors(
    app: FastAPI,
    origins: Sequence[str],
    *,
    allow_credentials: bool = False,
    max_age: int = 600,
) -> None:
    """Allow browser clients served from ``origins`` to use the transfer API.

    Sets the request and response headers the xferkit browser client needs.
    Set ``allow_credentials`` if the browser authenticates with cookies.
    """
    from fastapi.middleware.cors import CORSMiddleware

    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(origins),
        allow_methods=["GET", "HEAD", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=CORS_ALLOW_HEADERS,
        expose_headers=CORS_EXPOSE_HEADERS,
        allow_credentials=allow_credentials,
        max_age=max_age,
    )


def mount_browser_client(app: FastAPI | APIRouter, path: str = "/xferkit.mjs") -> None:
    """Serve the bundled browser client (an ES module) at ``path``::

    mount_browser_client(app)
    # <script type="module">
    #   import { upload } from "/xferkit.mjs";
    # </script>
    """
    from ..static import BROWSER_CLIENT

    @app.get(path, include_in_schema=False)
    async def browser_client(request: Request) -> Response:
        return range_file_response(
            request,
            BROWSER_CLIENT,
            media_type="text/javascript; charset=utf-8",
            disposition=None,
            headers={"Cache-Control": "public, max-age=3600"},
        )


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


def require_grant(signer: GrantSigner) -> Callable[[Request], Awaitable[Grant]]:
    """FastAPI dependency protecting your own routes (e.g. a
    :func:`range_file_response` route) with signed grants::

        @app.get("/files/{name}")
        async def get_file(name: str, request: Request,
                           grant: Grant = Depends(require_grant(signer))):
            ...
    """

    async def dependency(request: Request) -> Grant:
        token = request.headers.get(GRANT_HEADER) or request.query_params.get(GRANT_QUERY)
        if not token:
            raise HTTPException(status_code=401, detail="a signed grant is required")
        try:
            return signer.verify(token, method=request.method, path=request.url.path)
        except InvalidGrant as exc:
            raise HTTPException(status_code=401, detail=exc.message) from None

    return dependency
