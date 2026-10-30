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
into a file that is being moved. Locks older than a timeout are treated as left
behind by a crashed process and taken over.

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

## Client resume

- **Upload:** an append-only journal (`state_dir/upload-<hash>.journal`)
  holds the session id and one line per acknowledged part with its digest.
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

## Known limits / roadmap

- Only `LocalStorage` ships today. `StorageBackend` is the extension point for
  S3-compatible object storage (which maps naturally to multipart upload).
- No built-in authentication: protect the router with FastAPI dependencies.
- Windows servers are not yet covered by CI.
- Possible additions: tus adapter for browser clients, a browser JS client,
  upload-size quotas per user, Prometheus metrics hooks.
