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
