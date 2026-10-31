# xferkit — browser client

Parallel, resumable, verified uploads and downloads from the browser to an
[xferkit](https://github.com/SeaViewer91/xferkit) server. No dependencies, ~18 KB.

- Uploads files in parallel parts; every part is SHA-256 checked by the server
  and the whole file is verified end to end with a composite checksum.
- Resumes after a reload, a dropped connection or `AbortController.abort()`:
  call `upload()` again with the same file.
- `idempotencyKey` guarantees one server-side copy per key — the same key means
  the same upload in the Python client too.
- Downloads with parallel range requests into a `Blob` or straight to disk
  (File System Access API).
- Signed grants and custom headers for authentication, `credentials` for cookies.

## Get it

Without a build step, from your xferkit FastAPI server:

```python
from xferkit.integrations.fastapi import mount_browser_client
mount_browser_client(app)          # serves /xferkit.mjs
```

```html
<script type="module">
  import { upload } from "/xferkit.mjs";
</script>
```

Or from npm (`npm install xferkit`) in a bundled app: `import { upload } from "xferkit"`.

## Upload

```js
import { upload } from "/xferkit.mjs";

const controller = new AbortController();
const result = await upload(file, {
  baseUrl: "https://files.example.com/uploads",
  grant,                                  // signed grant from your app (or headers / credentials)
  metadata: { project: "coastal-2026" },
  idempotencyKey: `coastal-2026/${file.name}`,   // optional: never upload twice
  concurrency: 4,
  signal: controller.signal,
  onProgress: ({ fraction }) => (bar.value = fraction),
});
console.log(result.uploadId, result.resumed, result.compositeChecksum);
```

Progress is reported per part (8 MiB by default). Resume state lives in
`localStorage` under `xferkit:upload:*` and is removed after success; pass
`resume: false` or your own `{ get, set, remove }` store to change that.

## Download

```js
import { download, saveUrl } from "/xferkit.mjs";

// Into memory (fine up to a few hundred MB):
const blob = await download(url, { concurrency: 4, checksum: "sha256=..." });

// Straight to disk, any size (Chromium-based browsers):
const handle = await showSaveFilePicker({ suggestedName: "scene.tif" });
await download(url, { target: handle, onProgress });

// Or let the browser's download manager do it (any browser, any size):
saveUrl(signedUrl, "scene.tif");
```

## Errors

Failures throw `XferkitError` with the server's `code` (`invalid_grant`,
`upload_not_found`, `quota_exceeded`, `insufficient_storage`, ...), the HTTP
`status` and `details`. Cancelling throws the `AbortError` of your signal.
Network errors and 408/429/5xx responses are retried with back-off, honouring
`Retry-After`.

## Cross-origin

If the page and the transfer server have different origins, enable CORS on the
server:

```python
from xferkit.integrations.fastapi import add_cors
add_cors(app, ["https://app.example.com"])
```

## Develop

```bash
npm install
npm run build      # type-check, bundle to dist/, copy the bundle into the Python package
```

The browser tests live in the Python test suite (`tests/test_browser.py`,
Playwright + Chromium).
