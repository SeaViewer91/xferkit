// src/common.ts
var PROTOCOL_HEADER = "X-Xferkit-Protocol";
var CHECKSUM_HEADER = "X-Xferkit-Checksum";
var GRANT_HEADER = "X-Xferkit-Grant";
var XferkitError = class extends Error {
  constructor(message, code = "transfer_error", status = 0, details = {}) {
    super(message);
    this.name = "XferkitError";
    this.code = code;
    this.status = status;
    this.details = details;
  }
};
var RETRYABLE = /* @__PURE__ */ new Set([408, 425, 429, 500, 502, 503, 504]);
function progressOf(done, total) {
  return { bytesDone: done, bytesTotal: total, fraction: total === 0 ? 1 : done / total };
}
function throwIfAborted(signal) {
  if (signal?.aborted) {
    throw signal.reason instanceof Error ? signal.reason : new DOMException("Aborted", "AbortError");
  }
}
function sleep(ms, signal) {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(signal.reason);
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal.reason);
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}
function backoff(attempt, retry, retryAfter) {
  if (retryAfter) {
    const seconds = Number(retryAfter);
    const ms = Number.isFinite(seconds) ? seconds * 1e3 : Date.parse(retryAfter) - Date.now();
    if (Number.isFinite(ms) && ms >= 0) return Math.min(ms, retry.maxBackoffMs);
  }
  const base = Math.min(retry.maxBackoffMs, retry.backoffMs * 2 ** attempt);
  return base * (0.5 + Math.random() / 2);
}
function retryDefaults(retry) {
  return { attempts: 6, backoffMs: 500, maxBackoffMs: 3e4, ...retry };
}
function buildHeaders(conn, extra) {
  const headers = { ...conn.headers, ...extra };
  if (conn.grant) headers[GRANT_HEADER] = conn.grant;
  return headers;
}
async function errorFromResponse(response) {
  let body = null;
  try {
    body = await response.json();
  } catch {
  }
  const err = body?.error;
  if (err && typeof err === "object") {
    return new XferkitError(err.message ?? `HTTP ${response.status}`, err.code ?? "transfer_error", response.status, err.details ?? {});
  }
  return new XferkitError(`unexpected HTTP ${response.status}`, "transfer_error", response.status);
}
async function fetchWithRetry(conn, url, init, extraRetryStatuses = []) {
  const retry = retryDefaults(conn.retry);
  const doFetch = conn.fetch ?? globalThis.fetch.bind(globalThis);
  for (let attempt = 0; ; attempt++) {
    throwIfAborted(conn.signal);
    const last = attempt >= retry.attempts - 1;
    let response;
    try {
      response = await doFetch(url, { credentials: conn.credentials, ...init, signal: conn.signal });
    } catch (error) {
      if (conn.signal?.aborted || last) throw error;
      await sleep(backoff(attempt, retry, null), conn.signal);
      continue;
    }
    if (!last && (RETRYABLE.has(response.status) || extraRetryStatuses.includes(response.status))) {
      await response.body?.cancel().catch(() => void 0);
      await sleep(backoff(attempt, retry, response.headers.get("Retry-After")), conn.signal);
      continue;
    }
    return response;
  }
}
async function fetchJson(conn, url, init, extraRetryStatuses = []) {
  const response = await fetchWithRetry(conn, url, init, extraRetryStatuses);
  if (!response.ok) throw await errorFromResponse(response);
  return { data: await response.json(), response };
}
async function sha256(data) {
  return new Uint8Array(await crypto.subtle.digest("SHA-256", data));
}
function toHex(bytes) {
  let out = "";
  for (const b of bytes) out += b.toString(16).padStart(2, "0");
  return out;
}
function base64url(bytes) {
  let binary = "";
  for (const b of bytes) binary += String.fromCharCode(b);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}
function randomToken(bytes = 18) {
  return base64url(crypto.getRandomValues(new Uint8Array(bytes)));
}
async function tokenForKey(key) {
  const prefix = new TextEncoder().encode("xferkit-idempotency\0");
  const body = new TextEncoder().encode(key);
  const joined = new Uint8Array(prefix.length + body.length);
  joined.set(prefix);
  joined.set(body, prefix.length);
  return base64url(await sha256(joined));
}
async function compositeChecksum(digests) {
  const joined = new Uint8Array(digests.length * 32);
  digests.forEach((d, i) => joined.set(d, i * 32));
  return `sha256=${toHex(await sha256(joined))}`;
}
async function runPool(items, concurrency, worker) {
  const queue = items.slice();
  let failure = null;
  const runners = Array.from({ length: Math.min(concurrency, queue.length) }, async () => {
    while (queue.length && failure === null) {
      const item = queue.shift();
      try {
        await worker(item);
      } catch (error) {
        if (failure === null) failure = error;
      }
    }
  });
  await Promise.all(runners);
  if (failure !== null) throw failure;
}

// src/upload.ts
var PART_RETRY_STATUSES = [409, 422];
function defaultStore() {
  try {
    const storage = globalThis.localStorage;
    const probe2 = "__xferkit_probe__";
    storage.setItem(probe2, "1");
    storage.removeItem(probe2);
    return {
      get: (k) => storage.getItem(k),
      set: (k, v) => storage.setItem(k, v),
      remove: (k) => storage.removeItem(k)
    };
  } catch {
    return null;
  }
}
function resolveStore(resume) {
  if (resume === false) return null;
  if (resume === void 0 || resume === true) return defaultStore();
  return resume;
}
function join(base, ...parts) {
  return [base.replace(/\/+$/, ""), ...parts.map(encodeURIComponent)].join("/");
}
async function journalKey(baseUrl, file, filename, idempotencyKey) {
  const lastModified = file.lastModified ?? 0;
  const identity = `${baseUrl}
${filename}
${file.size}
${lastModified}
${idempotencyKey ?? ""}`;
  return `xferkit:upload:${toHex(await sha256(new TextEncoder().encode(identity))).slice(0, 32)}`;
}
async function getUploadStatus(baseUrl, uploadId, options = {}) {
  const { data } = await fetchJson(options, join(baseUrl, uploadId), {
    method: "GET",
    headers: buildHeaders(options)
  });
  return data;
}
async function abortUpload(baseUrl, uploadId, options = {}) {
  const response = await fetchWithRetry(options, join(baseUrl, uploadId), {
    method: "DELETE",
    headers: buildHeaders(options)
  });
  if (!response.ok && response.status !== 404) throw await errorFromResponse(response);
}
async function upload(file, options) {
  const baseUrl = options.baseUrl.replace(/\/+$/, "");
  const filename = options.filename ?? file.name ?? "blob";
  const size = file.size;
  const store = resolveStore(options.resume);
  const key = await journalKey(baseUrl, file, filename, options.idempotencyKey);
  const keyToken = options.idempotencyKey !== void 0 ? await tokenForKey(options.idempotencyKey) : void 0;
  let journal = null;
  if (store && !options.uploadId) {
    try {
      journal = JSON.parse(store.get(key) ?? "null");
    } catch {
      journal = null;
    }
    if (journal && keyToken && journal.clientToken !== keyToken) journal = null;
  }
  const saveJournal = (j) => store?.set(key, JSON.stringify(j));
  let status = null;
  let resumed = false;
  const knownId = options.uploadId ?? journal?.uploadId;
  if (knownId) {
    try {
      status = await getUploadStatus(baseUrl, knownId, options);
      resumed = true;
    } catch (error) {
      const notFound = error instanceof XferkitError && (error.status === 404 || error.status === 410);
      if (!notFound || options.uploadId) throw error;
    }
  }
  if (status === null) {
    const clientToken = keyToken ?? journal?.clientToken ?? randomToken();
    saveJournal({ clientToken });
    const body = {
      filename,
      size,
      metadata: options.metadata ?? {},
      client_token: clientToken
    };
    if (options.chunkSize !== void 0) body.chunk_size = options.chunkSize;
    const { data, response: response2 } = await fetchJson(options, baseUrl, {
      method: "POST",
      headers: buildHeaders(options, { "Content-Type": "application/json" }),
      body: JSON.stringify(body)
    });
    status = data;
    resumed = response2.status === 200;
    saveJournal({ clientToken, uploadId: status.upload_id });
  }
  if (status.size !== size) {
    throw new XferkitError(`upload ${status.upload_id} expects ${status.size} bytes, the file has ${size}`, "invalid_request");
  }
  const uploadId = status.upload_id;
  const chunkSize = status.chunk_size;
  const count = size === 0 ? 0 : Math.ceil(size / chunkSize);
  const partLength = (i) => Math.min(chunkSize, size - i * chunkSize);
  const digests = /* @__PURE__ */ new Map();
  const received = new Set(status.state === "completed" ? Array.from({ length: count }, (_, i) => i) : status.received_parts ?? []);
  let done = 0;
  for (const i of received) done += partLength(i);
  options.onProgress?.(progressOf(done, size));
  const sendParts = async (indices) => {
    await runPool(indices, Math.max(1, options.concurrency ?? 4), async (index) => {
      throwIfAborted(options.signal);
      const start = index * chunkSize;
      const slice = file.slice(start, start + partLength(index));
      const digest = await sha256(await slice.arrayBuffer());
      await fetchJson(
        options,
        join(baseUrl, uploadId, "parts", String(index)),
        {
          method: "PUT",
          headers: buildHeaders(options, {
            "Content-Type": "application/octet-stream",
            [CHECKSUM_HEADER]: `sha256=${toHex(digest)}`
          }),
          // A Blob body is streamed from the file by the browser; an ArrayBuffer
          // body is copied between processes and is ~8x slower in Chromium.
          body: slice
        },
        PART_RETRY_STATUSES
      );
      digests.set(index, digest);
      done += slice.size;
      options.onProgress?.(progressOf(done, size));
    });
  };
  let response;
  if (status.state === "completed") {
    response = status;
  } else {
    await sendParts(Array.from({ length: count }, (_, i) => i).filter((i) => !received.has(i)));
    response = await completeWithRecovery(baseUrl, uploadId, options, async (missing) => {
      for (const i of missing) {
        done -= partLength(i);
        digests.delete(i);
      }
      await sendParts(missing);
    });
  }
  for (let i = 0; i < count; i++) {
    if (!digests.has(i)) {
      const start = i * chunkSize;
      digests.set(i, await sha256(await file.slice(start, start + partLength(i)).arrayBuffer()));
    }
  }
  const local = await compositeChecksum(Array.from({ length: count }, (_, i) => digests.get(i)));
  if (response.composite_checksum !== local) {
    throw new XferkitError("the server's composite checksum does not match the local file", "integrity_error", 0, {
      local,
      remote: response.composite_checksum
    });
  }
  store?.remove(key);
  return {
    uploadId,
    filename: response.filename ?? filename,
    size,
    compositeChecksum: local,
    resumed,
    metadata: response.metadata ?? {},
    response
  };
}
async function completeWithRecovery(baseUrl, uploadId, options, resend) {
  const attempts = options.retry?.attempts ?? 6;
  for (let attempt = 0; ; attempt++) {
    try {
      const { data } = await fetchJson(options, join(baseUrl, uploadId, "complete"), {
        method: "POST",
        headers: buildHeaders(options)
      });
      return data;
    } catch (error) {
      if (!(error instanceof XferkitError) || attempt >= attempts - 1) throw error;
      if (error.code === "upload_incomplete") {
        const missing = error.details.missing_parts ?? [];
        if (!missing.length) throw error;
        await resend(missing);
      } else if (error.code === "upload_conflict") {
        await new Promise((r) => setTimeout(r, 500 * 2 ** attempt));
      } else {
        throw error;
      }
    }
  }
}
var Uploader = class {
  constructor(defaults) {
    this.defaults = defaults;
  }
  upload(file, options = {}) {
    return upload(file, { ...this.defaults, ...options });
  }
  status(uploadId) {
    return getUploadStatus(this.defaults.baseUrl, uploadId, this.defaults);
  }
  abort(uploadId) {
    return abortUpload(this.defaults.baseUrl, uploadId, this.defaults);
  }
};

// src/download.ts
var RemoteChanged = class extends XferkitError {
  constructor() {
    super("the remote file changed during the download", "remote_changed", 412);
  }
};
var CONTENT_RANGE = /^\s*bytes\s+(?:(\d+)-(\d+)|\*)\/(\d+|\*)\s*$/i;
function parseContentRange(value) {
  const m = CONTENT_RANGE.exec(value ?? "");
  if (!m) return [null, null, null];
  return [m[1] ? Number(m[1]) : null, m[2] ? Number(m[2]) : null, m[3] && m[3] !== "*" ? Number(m[3]) : null];
}
async function probe(url, options) {
  const response = await fetchWithRetry(options, url, {
    method: "GET",
    headers: buildHeaders(options, { Range: "bytes=0-0" })
  });
  const etag = response.headers.get("ETag");
  const lastModified = response.headers.get("Last-Modified");
  const validator = etag && !etag.startsWith("W/") ? etag : lastModified;
  const contentType = response.headers.get("Content-Type") ?? "application/octet-stream";
  await response.body?.cancel().catch(() => void 0);
  if (response.status === 206) {
    const total = parseContentRange(response.headers.get("Content-Range"))[2];
    return { size: total, ranged: total !== null, validator, contentType };
  }
  if (response.status === 416) {
    return { size: parseContentRange(response.headers.get("Content-Range"))[2] ?? 0, ranged: false, validator, contentType };
  }
  if (response.status === 200) {
    const length = response.headers.get("Content-Length");
    return { size: length ? Number(length) : null, ranged: false, validator, contentType };
  }
  throw await errorFromResponse(response);
}
async function openWriter(target) {
  if (!target) return null;
  if ("createWritable" in target) return target.createWritable({ keepExistingData: false });
  return target;
}
async function download(url, options = {}) {
  try {
    return await downloadOnce(url, options);
  } catch (error) {
    if (error instanceof RemoteChanged) return downloadOnce(url, options);
    throw error;
  }
}
async function downloadOnce(url, options) {
  const info = await probe(url, options);
  const writer = await openWriter(options.target);
  try {
    let blob;
    if (!info.ranged || !info.size) {
      blob = await downloadWhole(url, options, info);
      if (writer) {
        await writer.write({ type: "write", position: 0, data: await blob.arrayBuffer() });
      }
    } else {
      const parts = await downloadRanged(url, options, info, writer);
      blob = new Blob(parts, { type: info.contentType });
    }
    if (options.checksum && !writer) {
      const actual = `sha256=${toHex(await sha256(await blob.arrayBuffer()))}`;
      if (actual !== options.checksum.toLowerCase()) {
        throw new XferkitError("the downloaded file does not match the expected checksum", "integrity_error", 0, {
          expected: options.checksum,
          actual
        });
      }
    }
    if (writer) {
      await writer.close();
      return void 0;
    }
    return blob;
  } catch (error) {
    await writer?.abort?.(error).catch(() => void 0);
    throw error;
  }
}
async function downloadWhole(url, options, info) {
  const response = await fetchWithRetry(options, url, { method: "GET", headers: buildHeaders(options) });
  if (response.status !== 200) throw await errorFromResponse(response);
  const total = info.size ?? Number(response.headers.get("Content-Length") ?? 0);
  if (!response.body) return response.blob();
  const reader = response.body.getReader();
  const chunks = [];
  let done = 0;
  for (; ; ) {
    const { value, done: finished } = await reader.read();
    if (finished) break;
    chunks.push(value);
    done += value.byteLength;
    options.onProgress?.(progressOf(done, total));
  }
  return new Blob(chunks, { type: info.contentType });
}
async function downloadRanged(url, options, info, writer) {
  const size = info.size;
  const step = Math.max(1, options.segmentSize ?? 8 * 1024 * 1024);
  const segments = [];
  for (let i = 0, start = 0; start < size; i++, start += step) {
    segments.push([i, start, Math.min(start + step, size) - 1]);
  }
  const parts = new Array(segments.length);
  let done = 0;
  options.onProgress?.(progressOf(0, size));
  await runPool(segments, Math.max(1, options.concurrency ?? 4), async ([index, start, end]) => {
    throwIfAborted(options.signal);
    const headers = { Range: `bytes=${start}-${end}` };
    if (info.validator) headers["If-Range"] = info.validator;
    for (let attempt = 0; ; attempt++) {
      const response = await fetchWithRetry(options, url, { method: "GET", headers: buildHeaders(options, headers) });
      if (response.status === 200) {
        await response.body?.cancel().catch(() => void 0);
        throw new RemoteChanged();
      }
      if (response.status !== 206) throw await errorFromResponse(response);
      const [from, to, total] = parseContentRange(response.headers.get("Content-Range"));
      if (total !== null && total !== size) throw new RemoteChanged();
      if (from !== start || to !== end) {
        throw new XferkitError(`server sent range ${from}-${to}, expected ${start}-${end}`, "transfer_error");
      }
      let data;
      try {
        data = await response.arrayBuffer();
      } catch (error) {
        if (options.signal?.aborted || attempt >= 4) throw error;
        continue;
      }
      if (data.byteLength !== end - start + 1) {
        if (attempt >= 4) throw new XferkitError("segment arrived incomplete", "transfer_error");
        continue;
      }
      if (writer) await writer.write({ type: "write", position: start, data });
      else parts[index] = data;
      done += data.byteLength;
      options.onProgress?.(progressOf(done, size));
      return;
    }
  });
  return writer ? [] : parts;
}
function saveUrl(url, filename) {
  const link = document.createElement("a");
  link.href = url;
  if (filename) link.download = filename;
  link.rel = "noopener";
  link.style.display = "none";
  document.body.appendChild(link);
  link.click();
  link.remove();
}

// src/index.ts
var VERSION = "0.1.0";
export {
  CHECKSUM_HEADER,
  GRANT_HEADER,
  PROTOCOL_HEADER,
  Uploader,
  VERSION,
  XferkitError,
  abortUpload,
  download,
  getUploadStatus,
  saveUrl,
  upload
};
//# sourceMappingURL=xferkit.mjs.map
