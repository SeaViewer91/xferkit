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
    const { data, response } = await fetchJson(options, baseUrl, {
      method: "POST",
      headers: buildHeaders(options, { "Content-Type": "application/json" }),
      body: JSON.stringify(body)
    });
    status = data;
    resumed = response.status === 200;
    saveJournal({ clientToken, uploadId: status.upload_id });
  }
  let done = 0;
  const result = await transfer(file, filename, status, resumed, options, (delta) => {
    done += delta;
    options.onProgress?.(progressOf(done, size));
  });
  store?.remove(key);
  return result;
}
var Limiter = class {
  constructor(limit) {
    this.limit = limit;
    this.active = 0;
    this.waiting = [];
  }
  async run(task) {
    if (this.active >= this.limit) await new Promise((resolve) => this.waiting.push(resolve));
    else this.active++;
    try {
      return await task();
    } finally {
      const next = this.waiting.shift();
      if (next) next();
      else this.active--;
    }
  }
};
async function transfer(file, filename, status, resumed, options, onBytes, limiter) {
  const baseUrl = options.baseUrl.replace(/\/+$/, "");
  const size = file.size;
  if (status.size !== size) {
    throw new XferkitError(`upload ${status.upload_id} expects ${status.size} bytes, the file has ${size}`, "invalid_request");
  }
  const uploadId = status.upload_id;
  const chunkSize = status.chunk_size;
  const count = size === 0 ? 0 : Math.ceil(size / chunkSize);
  const partLength = (i) => Math.min(chunkSize, size - i * chunkSize);
  const digests = /* @__PURE__ */ new Map();
  const received = new Set(status.state === "completed" ? Array.from({ length: count }, (_, i) => i) : status.received_parts ?? []);
  let initial = 0;
  for (const i of received) initial += partLength(i);
  onBytes(initial);
  let completed = null;
  const sendOne = async (index) => {
    throwIfAborted(options.signal);
    const start = index * chunkSize;
    const slice = file.slice(start, start + partLength(index));
    const digest = await sha256(await slice.arrayBuffer());
    const { data } = await fetchJson(
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
    if (data && typeof data.completed === "object") completed = data.completed;
    onBytes(slice.size);
  };
  const sendParts = (indices) => runPool(indices, Math.max(1, options.concurrency ?? 4), (index) => limiter ? limiter.run(() => sendOne(index)) : sendOne(index));
  let response;
  if (status.state === "completed") {
    response = status;
  } else {
    await sendParts(Array.from({ length: count }, (_, i) => i).filter((i) => !received.has(i)));
    response = completed ?? await completeWithRecovery(baseUrl, uploadId, options, async (missing) => {
      for (const i of missing) {
        onBytes(-partLength(i));
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

// src/batch.ts
var batchSupport = /* @__PURE__ */ new Map();
function splitPath(entry) {
  if (entry.relativePath !== void 0) return { folder: entry.folder ?? "", relativePath: entry.relativePath };
  const webkit = entry.file.webkitRelativePath;
  if (webkit) {
    const slash = webkit.indexOf("/");
    return { folder: webkit.slice(0, slash), relativePath: webkit.slice(slash + 1) };
  }
  return { folder: entry.folder ?? "", relativePath: entry.file.name ?? "blob" };
}
function dedupeKey(base, filename, file) {
  const modified = Math.floor(file.lastModified ?? 0);
  return `xferkit-file
${base}
${filename}
${file.size}
${modified}`;
}
function isFileEntry(value) {
  return !(value instanceof Blob);
}
async function uploadFiles(files, options) {
  const baseUrl = options.baseUrl.replace(/\/+$/, "");
  const dedupe = options.dedupe ?? true;
  const filesAtOnce = Math.max(1, options.fileConcurrency ?? 6);
  const batchSize = Math.max(1, options.batchSize ?? 100);
  const autoCompleteBelow = options.autoCompleteBelow ?? 64 * 1024 * 1024;
  const skipHidden = options.skipHidden ?? true;
  const planned = [];
  for (const raw of Array.from(files)) {
    const entry = isFileEntry(raw) ? raw : { file: raw };
    const { folder, relativePath } = splitPath(entry);
    if (skipHidden && folder && relativePath.split("/").some((part) => part.startsWith("."))) continue;
    const filename = entry.filename ?? entry.file.name ?? relativePath.split("/").pop() ?? "blob";
    let token;
    if (entry.idempotencyKey !== void 0) token = await tokenForKey(entry.idempotencyKey);
    else if (dedupe) {
      const prefix = options.keyPrefix ?? (folder ? `${folder}/` : "");
      token = await tokenForKey(dedupeKey(prefix + relativePath, filename, entry.file));
    } else token = randomToken();
    const metadata = { ...options.metadata, ...entry.metadata };
    if (folder || relativePath !== filename) metadata.relative_path = relativePath;
    planned.push({ index: planned.length, file: entry.file, filename, relativePath, metadata, token });
  }
  const total = planned.reduce((sum, p) => sum + p.file.size, 0);
  let bytesDone = 0;
  let filesDone = 0;
  let filesFailed = 0;
  const report = () => options.onProgress?.({
    bytesDone,
    bytesTotal: total,
    fraction: total === 0 ? 1 : bytesDone / total,
    filesDone,
    filesFailed,
    filesTotal: planned.length
  });
  const uploaded = /* @__PURE__ */ new Map();
  const failed = /* @__PURE__ */ new Map();
  const fail = (item, error, unsent) => {
    const outcome = { file: item.file, relativePath: item.relativePath, error };
    failed.set(item.index, outcome);
    filesFailed++;
    bytesDone += unsent;
    report();
    options.onFile?.(outcome);
  };
  const limiter = new Limiter(Math.max(1, options.concurrency ?? 4));
  const transferOptions = { ...options, onProgress: void 0 };
  const pending = planned.slice();
  const ready = [];
  const running = /* @__PURE__ */ new Set();
  let throttled = false;
  let backoff2 = 0;
  const attempts = options.retry?.attempts ?? 6;
  const start = (item, status, created) => {
    let sent = 0;
    const task = (async () => {
      try {
        const result = await transfer(
          item.file,
          item.filename,
          status,
          !created,
          transferOptions,
          (delta) => {
            sent += delta;
            bytesDone += delta;
            report();
          },
          limiter
        );
        const outcome = { ...result, relativePath: item.relativePath };
        uploaded.set(item.index, outcome);
        filesDone++;
        report();
        options.onFile?.(outcome);
      } catch (error) {
        if (options.signal?.aborted) throw error;
        fail(item, error, item.file.size - sent);
      }
    })();
    running.add(task);
    const settle = () => {
      running.delete(task);
      throttled = false;
    };
    task.then(settle, settle);
  };
  try {
    while (pending.length || ready.length || running.size) {
      throwIfAborted(options.signal);
      if (pending.length && ready.length < filesAtOnce && !(throttled && running.size)) {
        const room = Math.max(batchSize, filesAtOnce) - ready.length - running.size;
        const chunk = pending.splice(0, Math.min(room, batchSize));
        const retryLater = [];
        if (chunk.length) {
          const outcomes = await createMany(baseUrl, chunk, autoCompleteBelow, options);
          chunk.forEach((item, i) => {
            const outcome = outcomes[i];
            if ("error" in outcome) {
              const busy = outcome.error instanceof XferkitError && outcome.error.code === "too_many_uploads";
              if (busy) retryLater.push(item);
              else fail(item, outcome.error, item.file.size);
            } else ready.push({ item, ...outcome });
          });
        }
        throttled = retryLater.length > 0;
        if (retryLater.length) {
          pending.unshift(...retryLater);
          if (!running.size && !ready.length) {
            if (backoff2 >= attempts - 1) {
              const error = new XferkitError("the server accepts no more uploads now", "too_many_uploads", 429);
              for (const item of pending.splice(0)) fail(item, error, item.file.size);
              continue;
            }
            await new Promise((r) => setTimeout(r, Math.min(3e4, 500 * 2 ** backoff2)));
            backoff2++;
            continue;
          }
        } else backoff2 = 0;
      }
      while (ready.length && running.size < filesAtOnce) {
        const next = ready.shift();
        start(next.item, next.status, next.created);
      }
      if (running.size) await Promise.race(running);
    }
  } catch (error) {
    await Promise.allSettled(Array.from(running));
    throw error;
  }
  const byIndex = (map) => Array.from(map.keys()).sort((a, b) => a - b).map((k) => map.get(k));
  return { uploaded: byIndex(uploaded), failed: byIndex(failed), ok: failed.size === 0 };
}
async function createMany(baseUrl, items, autoCompleteBelow, options) {
  const bodies = items.map((item) => {
    const body = {
      filename: item.filename,
      size: item.file.size,
      metadata: item.metadata,
      client_token: item.token
    };
    if (options.chunkSize !== void 0) body.chunk_size = options.chunkSize;
    if (item.file.size <= autoCompleteBelow) body.auto_complete = true;
    return body;
  });
  const headers = buildHeaders(options, { "Content-Type": "application/json" });
  if (batchSupport.get(baseUrl) !== false) {
    let response;
    try {
      response = await fetchWithRetry(options, join(baseUrl, "batch"), {
        method: "POST",
        headers,
        body: JSON.stringify({ files: bodies })
      });
    } catch (error) {
      if (options.signal?.aborted) throw error;
      return items.map(() => ({ error }));
    }
    if ((response.status === 404 || response.status === 405) && !batchSupport.has(baseUrl)) {
      batchSupport.set(baseUrl, false);
      await response.body?.cancel().catch(() => void 0);
    } else if (!response.ok) {
      const error = await errorFromResponse(response);
      return items.map(() => ({ error }));
    } else {
      batchSupport.set(baseUrl, true);
      const { uploads } = await response.json();
      return uploads.map((entry) => {
        const error = entry.error;
        if (error) {
          return {
            error: new XferkitError(error.message ?? "failed", error.code ?? "transfer_error", Number(entry.status ?? 0), error.details ?? {})
          };
        }
        const { created, ...status } = entry;
        return { status, created: Boolean(created) };
      });
    }
  }
  return Promise.all(
    bodies.map(async (body) => {
      try {
        const { data, response } = await fetchJson(options, baseUrl, {
          method: "POST",
          headers,
          body: JSON.stringify(body)
        });
        return { status: data, created: response.status === 201 };
      } catch (error) {
        if (options.signal?.aborted) throw error;
        return { error };
      }
    })
  );
}
function filesFromDataTransfer(dataTransfer) {
  const roots = [];
  const loose = [];
  for (const item of Array.from(dataTransfer.items)) {
    if (item.kind !== "file") continue;
    const entry = item.webkitGetAsEntry?.();
    if (entry) roots.push(entry);
    else {
      const file = item.getAsFile();
      if (file) loose.push(file);
    }
  }
  return (async () => {
    const out = loose.map((file) => ({ file }));
    for (const root of roots) {
      if (root.isFile) out.push({ file: await entryFile(root) });
      else await walk(root, root.name, "", out);
    }
    return out;
  })();
}
function entryFile(entry) {
  return new Promise((resolve, reject) => entry.file(resolve, reject));
}
async function walk(directory, folder, prefix, out) {
  const reader = directory.createReader();
  const children = [];
  for (; ; ) {
    const batch = await new Promise((resolve, reject) => reader.readEntries(resolve, reject));
    if (!batch.length) break;
    children.push(...batch);
  }
  children.sort((a, b) => a.name < b.name ? -1 : a.name > b.name ? 1 : 0);
  for (const child of children) {
    const relativePath = prefix + child.name;
    if (child.isFile) out.push({ file: await entryFile(child), folder, relativePath });
    else await walk(child, folder, `${relativePath}/`, out);
  }
}

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
  filesFromDataTransfer,
  getUploadStatus,
  saveUrl,
  upload,
  uploadFiles
};
//# sourceMappingURL=xferkit.mjs.map
