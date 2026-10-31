// Parallel, resumable, verified uploads (xferkit protocol v1).

import {
  CHECKSUM_HEADER,
  type ConnectionOptions,
  type ProgressCallback,
  XferkitError,
  buildHeaders,
  compositeChecksum,
  errorFromResponse,
  fetchJson,
  fetchWithRetry,
  progressOf,
  randomToken,
  runPool,
  sha256,
  throwIfAborted,
  toHex,
  tokenForKey,
} from "./common";

/** Server view of an upload (pending, completing or completed). */
export interface UploadState {
  upload_id: string;
  filename: string;
  size: number;
  chunk_size: number;
  total_parts: number | null;
  state: "pending" | "completing" | "completed";
  received_parts?: number[];
  composite_checksum?: string;
  checksum?: string | null;
  metadata: Record<string, string>;
  owner?: string | null;
  [key: string]: unknown;
}

export interface UploadResult {
  uploadId: string;
  filename: string;
  size: number;
  compositeChecksum: string;
  /** True when an earlier, interrupted upload of the same file was continued. */
  resumed: boolean;
  metadata: Record<string, string>;
  response: UploadState;
}

/** Where resume information is kept between page loads. */
export interface ResumeStore {
  get(key: string): string | null;
  set(key: string, value: string): void;
  remove(key: string): void;
}

export interface UploadOptions extends ConnectionOptions {
  /** URL of the upload router, e.g. "https://files.example.com/uploads". */
  baseUrl: string;
  /** Name stored on the server (default: the File's name). */
  filename?: string;
  metadata?: Record<string, string>;
  /** Requested part size in bytes (default: the server decides). */
  chunkSize?: number;
  /** Parts in flight at once (default 4). Memory use is about concurrency × chunk size. */
  concurrency?: number;
  /** Any string identifying this upload in your application. Uploading again
   * with the same key returns the existing upload instead of a second copy. */
  idempotencyKey?: string;
  /** Continue an interrupted upload of the same file (default: localStorage). Pass
   * `false` to disable, or your own store. */
  resume?: boolean | ResumeStore;
  /** Continue this specific upload session. */
  uploadId?: string;
  /** Called after every part. */
  onProgress?: ProgressCallback;
}

export const PART_RETRY_STATUSES = [409, 422]; // part still locked server-side / corrupted in transit

interface Journal {
  clientToken: string;
  uploadId?: string;
}

function defaultStore(): ResumeStore | null {
  try {
    const storage = globalThis.localStorage;
    const probe = "__xferkit_probe__";
    storage.setItem(probe, "1");
    storage.removeItem(probe);
    return {
      get: (k) => storage.getItem(k),
      set: (k, v) => storage.setItem(k, v),
      remove: (k) => storage.removeItem(k),
    };
  } catch {
    return null; // private mode, sandboxed iframe, ...
  }
}

function resolveStore(resume: UploadOptions["resume"]): ResumeStore | null {
  if (resume === false) return null;
  if (resume === undefined || resume === true) return defaultStore();
  return resume;
}

export function join(base: string, ...parts: string[]): string {
  return [base.replace(/\/+$/, ""), ...parts.map(encodeURIComponent)].join("/");
}

async function journalKey(baseUrl: string, file: Blob, filename: string, idempotencyKey?: string): Promise<string> {
  const lastModified = (file as File).lastModified ?? 0;
  const identity = `${baseUrl}\n${filename}\n${file.size}\n${lastModified}\n${idempotencyKey ?? ""}`;
  return `xferkit:upload:${toHex(await sha256(new TextEncoder().encode(identity))).slice(0, 32)}`;
}

/** Status of an upload (pending or completed). */
export async function getUploadStatus(baseUrl: string, uploadId: string, options: ConnectionOptions = {}): Promise<UploadState> {
  const { data } = await fetchJson<UploadState>(options, join(baseUrl, uploadId), {
    method: "GET",
    headers: buildHeaders(options),
  });
  return data;
}

/** Abort an upload and discard its data on the server. */
export async function abortUpload(baseUrl: string, uploadId: string, options: ConnectionOptions = {}): Promise<void> {
  const response = await fetchWithRetry(options, join(baseUrl, uploadId), {
    method: "DELETE",
    headers: buildHeaders(options),
  });
  if (!response.ok && response.status !== 404) throw await errorFromResponse(response);
}

/**
 * Upload a File or Blob in parallel parts and return once the server has
 * assembled and verified it.
 *
 * Every part is SHA-256 checked by the server, and at the end the server's
 * composite checksum is compared with the one computed locally. If the page is
 * closed or the network drops, calling `upload` again with the same file
 * continues where it stopped (progress is kept in localStorage).
 */
export async function upload(file: Blob, options: UploadOptions): Promise<UploadResult> {
  const baseUrl = options.baseUrl.replace(/\/+$/, "");
  const filename = options.filename ?? (file as File).name ?? "blob";
  const size = file.size;
  const store = resolveStore(options.resume);
  const key = await journalKey(baseUrl, file, filename, options.idempotencyKey);
  const keyToken = options.idempotencyKey !== undefined ? await tokenForKey(options.idempotencyKey) : undefined;

  let journal: Journal | null = null;
  if (store && !options.uploadId) {
    try {
      journal = JSON.parse(store.get(key) ?? "null") as Journal | null;
    } catch {
      journal = null;
    }
    if (journal && keyToken && journal.clientToken !== keyToken) journal = null;
  }
  const saveJournal = (j: Journal) => store?.set(key, JSON.stringify(j));

  let status: UploadState | null = null;
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
    // The token is saved *before* the request: whatever happens next, a retry
    // with the same token finds this session instead of creating another one.
    const clientToken = keyToken ?? journal?.clientToken ?? randomToken();
    saveJournal({ clientToken });
    const body: Record<string, unknown> = {
      filename,
      size,
      metadata: options.metadata ?? {},
      client_token: clientToken,
    };
    if (options.chunkSize !== undefined) body.chunk_size = options.chunkSize;
    const { data, response } = await fetchJson<UploadState>(options, baseUrl, {
      method: "POST",
      headers: buildHeaders(options, { "Content-Type": "application/json" }),
      body: JSON.stringify(body),
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

/** Concurrency limit shared by several transfers. */
export class Limiter {
  private active = 0;
  private readonly waiting: (() => void)[] = [];

  constructor(private readonly limit: number) {}

  async run<T>(task: () => Promise<T>): Promise<T> {
    if (this.active >= this.limit) await new Promise<void>((resolve) => this.waiting.push(resolve));
    else this.active++;
    try {
      return await task();
    } finally {
      const next = this.waiting.shift();
      if (next) next();
      else this.active--;
    }
  }
}

/**
 * Send the parts the server is missing, complete and verify. `status` is the
 * session's server-side state (or its completion record). @internal
 */
export async function transfer(
  file: Blob,
  filename: string,
  status: UploadState,
  resumed: boolean,
  options: UploadOptions,
  onBytes: (delta: number) => void,
  limiter?: Limiter,
): Promise<UploadResult> {
  const baseUrl = options.baseUrl.replace(/\/+$/, "");
  const size = file.size;
  if (status.size !== size) {
    throw new XferkitError(`upload ${status.upload_id} expects ${status.size} bytes, the file has ${size}`, "invalid_request");
  }
  const uploadId = status.upload_id;
  const chunkSize = status.chunk_size;
  const count = size === 0 ? 0 : Math.ceil(size / chunkSize);
  const partLength = (i: number) => Math.min(chunkSize, size - i * chunkSize);
  const digests = new Map<number, Uint8Array>();
  const received = new Set<number>(status.state === "completed" ? Array.from({ length: count }, (_, i) => i) : status.received_parts ?? []);

  let initial = 0;
  for (const i of received) initial += partLength(i);
  onBytes(initial);

  // An auto_complete session answers its last part with the completion record.
  let completed = null as UploadState | null;
  const sendOne = async (index: number) => {
    throwIfAborted(options.signal);
    const start = index * chunkSize;
    const slice = file.slice(start, start + partLength(index));
    const digest = await sha256(await slice.arrayBuffer());
    const { data } = await fetchJson<{ completed?: UploadState }>(
      options,
      join(baseUrl, uploadId, "parts", String(index)),
      {
        method: "PUT",
        headers: buildHeaders(options, {
          "Content-Type": "application/octet-stream",
          [CHECKSUM_HEADER]: `sha256=${toHex(digest)}`,
        }),
        // A Blob body is streamed from the file by the browser; an ArrayBuffer
        // body is copied between processes and is ~8x slower in Chromium.
        body: slice,
      },
      PART_RETRY_STATUSES,
    );
    digests.set(index, digest);
    if (data && typeof data.completed === "object") completed = data.completed;
    onBytes(slice.size);
  };
  const sendParts = (indices: number[]) =>
    runPool(indices, Math.max(1, options.concurrency ?? 4), (index) => (limiter ? limiter.run(() => sendOne(index)) : sendOne(index)));

  let response: UploadState;
  if (status.state === "completed") {
    response = status; // finished earlier; we only verify below
  } else {
    await sendParts(Array.from({ length: count }, (_, i) => i).filter((i) => !received.has(i)));
    response =
      completed ??
      (await completeWithRecovery(baseUrl, uploadId, options, async (missing) => {
        for (const i of missing) {
          onBytes(-partLength(i)); // counted before, will be counted again when re-sent
          digests.delete(i);
        }
        await sendParts(missing);
      }));
  }

  // End-to-end check: hash of the part hashes, computed independently on both sides.
  for (let i = 0; i < count; i++) {
    if (!digests.has(i)) {
      const start = i * chunkSize;
      digests.set(i, await sha256(await file.slice(start, start + partLength(i)).arrayBuffer()));
    }
  }
  const local = await compositeChecksum(Array.from({ length: count }, (_, i) => digests.get(i)!));
  if (response.composite_checksum !== local) {
    throw new XferkitError("the server's composite checksum does not match the local file", "integrity_error", 0, {
      local,
      remote: response.composite_checksum,
    });
  }
  return {
    uploadId,
    filename: response.filename ?? filename,
    size,
    compositeChecksum: local,
    resumed,
    metadata: response.metadata ?? {},
    response,
  };
}

async function completeWithRecovery(
  baseUrl: string,
  uploadId: string,
  options: UploadOptions,
  resend: (missing: number[]) => Promise<void>,
): Promise<UploadState> {
  const attempts = options.retry?.attempts ?? 6;
  for (let attempt = 0; ; attempt++) {
    try {
      const { data } = await fetchJson<UploadState>(options, join(baseUrl, uploadId, "complete"), {
        method: "POST",
        headers: buildHeaders(options),
      });
      return data;
    } catch (error) {
      if (!(error instanceof XferkitError) || attempt >= attempts - 1) throw error;
      if (error.code === "upload_incomplete") {
        const missing = (error.details.missing_parts as number[] | undefined) ?? [];
        if (!missing.length) throw error;
        await resend(missing); // the server lost or rejected some parts
      } else if (error.code === "upload_conflict") {
        await new Promise((r) => setTimeout(r, 500 * 2 ** attempt)); // parts still being written
      } else {
        throw error;
      }
    }
  }
}

/** Upload helper bound to a server and default options. */
export class Uploader {
  constructor(private readonly defaults: UploadOptions) {}

  upload(file: Blob, options: Partial<UploadOptions> = {}): Promise<UploadResult> {
    return upload(file, { ...this.defaults, ...options });
  }

  status(uploadId: string): Promise<UploadState> {
    return getUploadStatus(this.defaults.baseUrl, uploadId, this.defaults);
  }

  abort(uploadId: string): Promise<void> {
    return abortUpload(this.defaults.baseUrl, uploadId, this.defaults);
  }
}
