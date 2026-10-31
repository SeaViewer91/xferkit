# xferkit upload protocol, version 1

All paths are relative to the router prefix (default `/uploads`). Every response
carries `X-Xferkit-Protocol: 1`. Request and response bodies are JSON except
part bodies, which are raw bytes.

## 0. Authentication and ownership

Servers decide how callers are identified. Two mechanisms are built in and may
be combined:

- **Application credentials** (cookies, `Authorization`, ...) that the server's
  application maps to an owner id.
- **Signed grants**, sent in the `X-Xferkit-Grant` header or the `xk` query
  parameter. A grant is `v1.<payload>.<mac>`: base64url JSON payload
  `{"o": owner, "p": path prefix, "m": "GET,HEAD,..." or "*", "e": expiry (unix
  seconds), "s": max upload size}` and its base64url HMAC-SHA256 under a secret
  shared by the issuing application and the transfer servers. A grant covers a
  request when the method is listed and the request path equals the prefix or
  lies below it (segment-wise). A grant whose prefix names one upload id grants
  access to that upload regardless of owner (download links).

Every upload has an `owner` (the caller that created it, or `null`). Requests
by any other caller are answered `404 upload_not_found`, exactly as for an
unknown id. Uploads with owner `null` are reachable by anyone who knows the id.

Servers may refuse anonymous callers (`401 unauthorized`), apply policies
(`403 forbidden` / `quota_exceeded`) and rate limits (`429` + `Retry-After`).

## 1. Create a session

```
POST /uploads
Content-Type: application/json

{
  "filename": "survey_2026.tif",
  "size": 53687091200,
  "chunk_size": 16777216,          // optional; server chooses if omitted
  "checksum": "sha256=<64 hex>",   // optional whole-file checksum
  "metadata": {"project": "coastal"},  // optional, string values only
  "client_token": "q3Z...random"       // optional, recommended (see below)
}
```

`201 Created`, `Location: /uploads/{upload_id}`

```json
{
  "upload_id": "Vb1k3s...",
  "filename": "survey_2026.tif",
  "size": 53687091200,
  "chunk_size": 16777216,
  "total_parts": 3200,
  "created_at": 1793400000.0,
  "expires_at": 1793486400.0,
  "checksum": null,
  "metadata": {"project": "coastal"},
  "owner": "alice",
  "state": "pending",
  "received_parts": []
}
```

**Idempotent creation.** `client_token` (8–128 chars of `[A-Za-z0-9_-]`) is a
random id the client generates and stores *before* sending the request. If the
same token is sent again, the server does not create a second session: it
answers `200 OK` with the existing upload's status (or its completion record
with `"state": "completed"`). This lets a client that crashed or lost the
response find its session again, and makes retrying this `POST` safe. Reusing a
token for a different file (other `filename` or `size`) is a `409`. Tokens are
scoped to the owner: two owners using the same token get different uploads.

A server may refuse a new upload with `413 payload_too_large` (file or grant
limit), `429 too_many_uploads` (too many unfinished uploads; retry later),
`403 quota_exceeded` (application policy) or `507 insufficient_storage`
(not enough free disk space for the declared size).

Parts are numbered `0 .. total_parts-1`. Part `n` covers bytes
`n*chunk_size .. min((n+1)*chunk_size, size) - 1`; every part except the last
is exactly `chunk_size` bytes.

When the client omits `chunk_size`, the server uses its default and grows it
(in whole MiB) if needed to stay under its `max_parts` limit.

## 2. Upload parts

```
PUT /uploads/{upload_id}/parts/{n}
Content-Length: <exact part size>
X-Xferkit-Checksum: sha256=<hex of this part>   (recommended)

<raw bytes>
```

`200 OK`

```json
{"index": 7, "size": 16777216, "checksum": "sha256=..."}
```

- Parts may be sent in any order and in parallel.
- Re-sending a part replaces it (it counts as missing until verified again).
- The same part must not be sent twice concurrently; the second request gets
  `409 upload_conflict` and can be retried.

## 3. Status (for resuming)

```
GET /uploads/{upload_id}
```

`200 OK` — the session object plus:

```json
{
  "state": "pending",            // or "completing"
  "received_parts": [0, 1, 2, 5],
  "missing_parts": [3, 4, 6],
  "received_bytes": 67108864
}
```

If the upload has already been completed, the same endpoint returns the
completion record (see below) with `"state": "completed"` plus `chunk_size` and
`total_parts`. A client that crashed after the server completed the upload but
before it saw the response uses this to finish verification instead of
uploading the file again.

## 4. Complete

```
POST /uploads/{upload_id}/complete
```

`200 OK`

```json
{
  "upload_id": "Vb1k3s...",
  "filename": "survey_2026.tif",
  "size": 53687091200,
  "composite_checksum": "sha256=...",
  "checksum": "sha256=..." ,      // set only if a whole-file checksum was declared and verified
  "created_at": 1793400000.0,
  "completed_at": 1793403600.0,
  "metadata": {"project": "coastal"}
}
```

Completion is idempotent: calling it again returns the same record.

**Composite checksum** = SHA-256 of the concatenation of the raw 32-byte
SHA-256 digests of all parts, in part order. The client computes the same value
from its own parts and compares, which verifies the whole file end to end
without re-reading it.

## 5. Abort

```
DELETE /uploads/{upload_id}
```

`204 No Content`. The session and its data are discarded.

## 6. Download a completed upload (optional)

Only when the router is built with `expose_downloads=True`:

```
GET|HEAD /uploads/{upload_id}/content
Range: bytes=0-16777215
If-Range: "<etag>"
```

Standard HTTP range semantics: `200`, `206 Partial Content`, `304`, `416`.

## Errors

```json
{"error": {"code": "upload_incomplete", "message": "2 of 3200 parts are missing",
           "details": {"missing_parts": [3, 4]}}}
```

| HTTP | code                    | meaning                                               | client action            |
|------|-------------------------|-------------------------------------------------------|--------------------------|
| 400  | `invalid_request`       | malformed request or value outside server limits      | fix the request          |
| 400  | `invalid_part`          | bad part index or wrong part length                   | fix the request          |
| 401  | `unauthorized`          | no credentials, and the server requires them          | authenticate             |
| 401  | `invalid_grant`         | grant forged, expired, or not covering this request   | get a new grant          |
| 403  | `forbidden`             | refused by the server's policy                        | —                        |
| 403  | `quota_exceeded`        | the upload exceeds the caller's quota                 | —                        |
| 404  | `upload_not_found`      | unknown id, or an upload owned by someone else        | create a new session     |
| 409  | `upload_conflict`       | part being written concurrently / completion running  | retry with back-off      |
| 409  | `upload_incomplete`     | `complete` with missing parts (`details.missing_parts`) | send them, complete again |
| 410  | `upload_expired`        | session past its expiry                               | create a new session     |
| 413  | `payload_too_large`     | file or part above limits                             | —                        |
| 416  | `range_not_satisfiable` | range outside the file                                | —                        |
| 422  | `checksum_mismatch`     | data does not match the declared checksum             | re-send the part         |
| 429  | `rate_limited`          | too many requests (`Retry-After` header)              | wait, retry              |
| 429  | `too_many_uploads`      | too many unfinished uploads (`Retry-After` header)    | wait or finish others    |
| 507  | `insufficient_storage`  | not enough free disk space on the server              | retry later              |

Request-body validation errors produced by FastAPI itself (e.g. negative
`size`) use FastAPI's standard `422` format.
