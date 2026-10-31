# Changelog

## 0.1.0 — unreleased

First version.

- Upload protocol v1: create / status / parts / complete / abort.
- `UploadManager` and `LocalStorage`: in-place part writes, per-part markers,
  multi-process safe, expiry and cleanup, idempotent completion.
- Per-part SHA-256, composite checksum, optional whole-file checksum.
- Range-request file serving helpers (`plan_file_response`, `iter_file_range`).
- FastAPI integration: `create_upload_router`, `range_file_response`.
- Async and sync clients with parallel, resumable uploads and downloads.
- `xferkit` CLI.
- Load test suite (`benchmarks/load`): ~10 MB synthetic images, 10 concurrent
  client sessions, multi-worker server, fault injection.
- Access control: uploads belong to their creator (`owner`); `principal=`,
  `require_auth=`, `authorize=` hooks on the FastAPI router; other callers get
  `404`.
- Signed grants (`GrantSigner`): stateless, scoped, expiring credentials and
  presigned URLs (`X-Xferkit-Grant` header or `xk` query parameter), secret
  rotation, per-grant upload size limit; `grant=` in the Python clients and CLI.
- Disk-space admission (`min_free_bytes`, accounting for unfinished uploads),
  optional `preallocate`, `507 insufficient_storage` on a full disk.
- Retention of completion records (`completed_ttl`, `remove_files_on_expiry`).
- Limits: `max_pending_uploads`, `max_pending_per_owner`,
  `max_pending_bytes_per_owner` (`429 too_many_uploads`), `before_create` hook,
  per-caller `RateLimiter` (`429` + `Retry-After`).
- Browser client (`clients/js`, TypeScript, also served by
  `mount_browser_client`): parallel resumable verified uploads, ranged
  downloads to Blob or disk, grants, abort/resume, idempotency keys shared
  with Python; `add_cors` helper.

### Fixed by the load test

- Part/completion locks left by a crashed server worker were only reclaimed
  after a 10-minute timeout, so uploads failed after a server restart. Locks
  now record hostname + pid and are reclaimed as soon as the owner is gone.
- A client that crashed after the server completed its upload created a
  duplicate upload when re-run. The status endpoint now answers for completed
  uploads (`state: "completed"`) and the client verifies instead of re-uploading.
- A client that crashed right after creating a session (before it recorded the
  upload id) left an orphaned session on the server. Session creation is now
  idempotent with a client-generated `client_token`; the server binds the
  token before creating the session and publishes sessions atomically, so a
  server crash mid-creation cannot orphan one either.
- A server crash while removing a finished session left a half-deleted
  directory under a valid id. Sessions are now renamed away before removal, and
  `cleanup_expired()` collects all crash debris.
- An application that crashed after `upload()` returned but before it recorded
  the result uploaded the file again on re-run. New
  `upload(..., idempotency_key=...)` (CLI `--idempotency-key`) maps an
  application key to the same server-side upload.
