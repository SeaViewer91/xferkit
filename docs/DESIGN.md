# Design notes

This document records the decisions behind xferkit and the guarantees they give.

## Goals

1. Move files of tens to hundreds of GB over ordinary HTTP(S), through proxies
   and load balancers, without special infrastructure.
2. Survive interruptions on either side and resume without re-sending data.
3. Detect corruption end to end.
4. Keep the server core dependency-free and framework-agnostic, so it can be
   embedded in FastAPI apps and in other Python server programs alike.

## Why a custom protocol instead of tus

[tus](https://tus.io) is a good protocol, but its core is sequential (one
offset per upload); parallel upload requires the `concatenation` extension,
which creates one upload per chunk and a final merge. xferkit's protocol is
closer to S3 multipart: fixed-size numbered parts, any order, any parallelism,
explicit completion. That maps directly to object storage backends later and
keeps the server simple. A tus adapter can be added on top if browser clients
(Uppy, tus-js-client) need it.

## Storage layout (LocalStorage)

```
uploads/<id>/session.json      written once at creation
uploads/<id>/data              pre-sized (sparse) file
uploads/<id>/parts/<n>.json    marker: part n verified (size, sha256)
uploads/<id>/parts/<n>.lock    part n is being written
uploads/<id>/.completing       completion in progress
uploads/.creating-<id>/        session being created (renamed into place)
uploads/.trash-<id>-<rand>/    session being removed
tokens/<client_token>          upload id created for that token
files/<id>/<filename>          result
records/<id>.json              completion record
```

### Parts are written in place

Every part has a fixed offset, so it is written directly at
`index * chunk_size` in one pre-sized file. Completion is an `os.replace`
of that file — instant, and peak disk usage equals the file size. The
alternative (one file per part, concatenated at the end) doubles disk usage
and makes completion of a 100 GB upload take minutes.

### State is per part, not a shared index

Nothing is ever read-modify-written by two requests. A part only touches its
own marker and lock files, and markers are written via temp file + rename.
This makes the store safe across threads **and** processes (several uvicorn
workers on one machine, or several machines on a shared POSIX filesystem)
without a database or an in-process lock.

### Locking order

- `open_part`: take `parts/<n>.lock` (O_EXCL), *then* check `.completing`.
- `finalize`: take `.completing` (O_EXCL), *then* check for any part locks.

Whichever happens second sees the other's lock, so a part can never be written
into a file that is being moved.

Each lock records `<hostname> <pid> <time>`. A lock is taken over immediately
when its owner process on the same host no longer exists (a worker killed
mid-part), and otherwise after a timeout (owners on other hosts sharing the
storage). Without the liveness check, a server crash blocked the affected
parts for the full timeout; the load test (S4) found this. On Windows only the
timeout applies, because `os.kill(pid, 0)` would terminate the process there.

### Re-uploading a part

Opening a part deletes its marker first. If the new body fails (wrong size,
bad checksum, client disconnect) the part simply stays missing and the client
sends it again; a half-written region is never reported as received.

## Integrity

- **Per part:** the client sends `X-Xferkit-Checksum: sha256=...`; the server
  hashes while streaming and rejects mismatches (`422`). The client retries.
- **Whole file, cheap:** the composite checksum (hash of part hashes) is
  computed by the server from its markers and by the client from its own
  parts; the client compares them after completion.
- **Whole file, strict (optional):** the client may declare a full SHA-256 at
  creation; the server re-reads the assembled file and verifies it before
  completing. This costs one extra full read on each side.

### Completed uploads stay addressable

After completion the session directory is removed, but the status endpoint
still answers for the id with the completion record (`state: "completed"`).
If a client dies after the server completed the upload but before it saw the
response, re-running it verifies the existing upload instead of creating a
duplicate (and firing `on_complete` twice). The load test (S4) found this.

### Idempotent session creation

A client crash between "server created the session" and "client recorded the
upload id" used to leave an orphaned session behind on every re-run (cleaned
only when it expired). The client now writes a random `client_token` to its
journal before sending the create request; the server binds the token to the
upload id (`tokens/<token>`, created with an atomic hard link) and answers a
repeated token with the existing upload. Token bindings are pruned by
`cleanup_expired()` once their upload no longer exists. The load test (S4)
found this.

Two server-side crash windows are closed the same way:

- The token is bound *before* the session is created. A crash in between
  leaves a token pointing at nothing, and the retry creates the session.
- A session is built in `uploads/.creating-<id>/` and renamed into place, so a
  crash mid-creation never exposes a half-written session.
- A finished or aborted session is renamed to `uploads/.trash-<id>-*` before it
  is deleted, so a crash mid-removal never leaves a half-deleted directory
  under a valid id.

Invariant: every `uploads/<id>/` directory is a complete, live session.
Everything else (`.creating-*`, `.trash-*`, stray `*.tmp`) is crash debris,
invisible to clients and removed by `cleanup_expired()` (staging directories
only after `garbage_age`, 1 hour, since they may still be in progress).

### Exactly-once from the application's point of view

The SDK removes its journal when `upload()` returns. If the *application*
crashes after that but before it records the result, a re-run cannot know the
file was already uploaded. `upload(..., idempotency_key=...)` closes this gap:
the key (any string meaningful to the application) is hashed into the
`client_token`, so the same key always maps to the same server-side upload,
pending or completed. The load test uses one key per file and session; it
found a duplicate in this window before the option existed.

## Client resume

- **Upload:** an append-only journal (`state_dir/upload-<hash>.journal`)
  holds the client token, the session id and one line per acknowledged part
  with its digest.
  Appending a line per part is O(1); rewriting a JSON document per part would
  be O(n²) for a 10 000-part upload. The journal is only reused if the file's
  path, size and mtime are unchanged. The server's status endpoint is the
  source of truth for which parts exist; the journal only saves re-hashing.
- **Download:** data goes to `<dest>.xferkit-part`, progress to
  `<dest>.xferkit-state` (same journal format). Segments are requested with
  `If-Range: <etag>`; if the server answers `200` instead of `206`, the file
  changed, and the download restarts from scratch once.

## Concurrency and memory

- Server: request bodies are streamed in ~1 MiB blocks to a thread pool; a
  part is never held fully in memory.
- Client: memory is about `concurrency × chunk_size` for uploads and
  `concurrency × 1 MiB` for downloads.

## Security model

- **Identity is the application's business.** The transfer server does not
  manage users. The router asks `principal(request)` who is calling, or accepts
  a signed grant issued by the application. Both yield an *owner* id.
- **Ownership instead of ACLs.** Every upload records its owner. All
  operations on it (status, parts, complete, abort, download) are refused with
  `404` for any other caller — the same answer as for an unknown id, so ids
  cannot be probed. Client tokens are hashed together with the owner, so one
  user can never reach another user's upload through a token either. Uploads
  without an owner keep the original "the id is the capability" behaviour.
- **Grants are stateless capabilities.** `v1.<payload>.<HMAC-SHA256>`, carrying
  owner, path prefix, methods, expiry and an optional upload size limit. They
  are verified with a shared secret, so any number of transfer servers can
  check them without a database, and the issuing application can live
  elsewhere (e.g. a cloud web app issuing grants for an on-premise transfer
  server). Prefixes match whole path segments (`/uploads` does not cover
  `/uploads2`). `previous_secrets` allows rotating the secret. Grants in URLs
  (`?xk=`) end up in logs and browser history, so keep their `ttl` short and
  their prefix narrow.
- **Policy hooks** — `authorize` (per request, async) and `before_create`
  (per new upload, sync) — let applications add quotas, file-type rules or
  roles without forking the router.
- **Rate limiting** is a per-process token bucket keyed by owner or client
  address; it protects the server from runaway clients rather than enforcing
  exact global quotas (use a gateway for that). Clients honour `Retry-After`.

## Disk space

Session data files are sparse: they occupy only what has been written. To
avoid accepting uploads that cannot finish, `LocalStorage` checks at creation
that `size + outstanding + min_free_bytes <= free`, where *outstanding* is the
space still needed by unfinished uploads (declared size minus allocated blocks,
read from `stat` — no index to keep in sync). Two uploads created at the same
instant may both pass the check, so `min_free_bytes` doubles as a safety
margin. With `preallocate=True` the space is reserved for real with
`posix_fallocate`. A full disk during a part write is reported as
`507 insufficient_storage` instead of a generic error.

## Retention

Completion records let crashed clients recognise finished uploads (and keep
`idempotency_key` working), so they cannot be dropped immediately. They expire
after `completed_ttl` (default 7 days); `remove_files_on_expiry` also deletes
the assembled file for deployments that serve files straight from the store.
Token bindings disappear with their upload.

## Browser client

The browser client (`clients/js`, TypeScript, no dependencies) implements the
same protocol and guarantees as the Python client. Two browser-specific
decisions:

- Each part is read once to hash it with WebCrypto, and the **`Blob` slice
  itself** is sent as the request body. Chromium copies `ArrayBuffer` bodies
  between processes and uploaded about 8× slower that way (17 vs 137 MiB/s in
  a micro-benchmark), while `Blob` bodies are streamed from the file.
- Resume state (client token and upload id) is kept in `localStorage`; part
  digests are not persisted but recomputed from the file when needed.
  Idempotency keys are hashed exactly like in Python, so the same key refers to
  the same upload from both clients.

Downloads go into a `Blob` or, for files larger than memory, into a
`FileSystemWritableFileStream` with positional writes; for arbitrary sizes in
any browser, a signed URL handed to the browser's own download manager
(`saveUrl`) is the simplest option.

## Known limits / roadmap

- Only `LocalStorage` ships today. `StorageBackend` is the extension point for
  S3-compatible object storage (which maps naturally to multipart upload).
- Owner liveness checks for crash recovery work per host; with several hosts
  sharing network storage, abandoned locks are reclaimed by timeout.
- Windows servers are not yet covered by CI.
- Possible additions: batch/folder transfers, S3 backend, tus adapter,
  Prometheus metrics hooks.
