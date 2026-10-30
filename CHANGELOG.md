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
