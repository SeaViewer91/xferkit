// Shared plumbing: errors, retries, hashing, encoding.

export const PROTOCOL_HEADER = "X-Xferkit-Protocol";
export const CHECKSUM_HEADER = "X-Xferkit-Checksum";
export const GRANT_HEADER = "X-Xferkit-Grant";

/** Error raised for failed transfers. `code` matches the server's error codes
 * (e.g. "upload_not_found", "invalid_grant", "insufficient_storage"). */
export class XferkitError extends Error {
  readonly code: string;
  readonly status: number;
  readonly details: Record<string, unknown>;

  constructor(message: string, code = "transfer_error", status = 0, details: Record<string, unknown> = {}) {
    super(message);
    this.name = "XferkitError";
    this.code = code;
    this.status = status;
    this.details = details;
  }
}

export interface TransferProgress {
  bytesDone: number;
  bytesTotal: number;
  /** 0..1 */
  fraction: number;
}

export type ProgressCallback = (progress: TransferProgress) => void;

export interface RetryOptions {
  /** Total attempts per request (default 6). */
  attempts?: number;
  /** First back-off in milliseconds (default 500), doubled each time. */
  backoffMs?: number;
  /** Longest back-off in milliseconds (default 30 000). */
  maxBackoffMs?: number;
}

export interface ConnectionOptions {
  /** Extra headers sent with every request (e.g. Authorization). */
  headers?: Record<string, string>;
  /** Signed grant issued by your application (sent as X-Xferkit-Grant). */
  grant?: string;
  /** fetch credentials mode; use "include" for cookie-authenticated APIs. */
  credentials?: RequestCredentials;
  retry?: RetryOptions;
  /** Cancels the transfer. An aborted upload can be resumed later. */
  signal?: AbortSignal;
  /** Custom fetch implementation (tests, Node, instrumentation). */
  fetch?: typeof fetch;
}

const RETRYABLE = new Set([408, 425, 429, 500, 502, 503, 504]);

export function progressOf(done: number, total: number): TransferProgress {
  return { bytesDone: done, bytesTotal: total, fraction: total === 0 ? 1 : done / total };
}

export function throwIfAborted(signal?: AbortSignal): void {
  if (signal?.aborted) {
    throw signal.reason instanceof Error ? signal.reason : new DOMException("Aborted", "AbortError");
  }
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(signal.reason);
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal!.reason);
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

function backoff(attempt: number, retry: Required<RetryOptions>, retryAfter: string | null): number {
  if (retryAfter) {
    const seconds = Number(retryAfter);
    const ms = Number.isFinite(seconds) ? seconds * 1000 : Date.parse(retryAfter) - Date.now();
    if (Number.isFinite(ms) && ms >= 0) return Math.min(ms, retry.maxBackoffMs);
  }
  const base = Math.min(retry.maxBackoffMs, retry.backoffMs * 2 ** attempt);
  return base * (0.5 + Math.random() / 2);
}

export function retryDefaults(retry?: RetryOptions): Required<RetryOptions> {
  return { attempts: 6, backoffMs: 500, maxBackoffMs: 30_000, ...retry };
}

export function buildHeaders(conn: ConnectionOptions, extra?: Record<string, string>): Record<string, string> {
  const headers: Record<string, string> = { ...conn.headers, ...extra };
  if (conn.grant) headers[GRANT_HEADER] = conn.grant;
  return headers;
}

export async function errorFromResponse(response: Response): Promise<XferkitError> {
  let body: unknown = null;
  try {
    body = await response.json();
  } catch {
    // not JSON
  }
  const err = (body as { error?: { code?: string; message?: string; details?: Record<string, unknown> } } | null)?.error;
  if (err && typeof err === "object") {
    return new XferkitError(err.message ?? `HTTP ${response.status}`, err.code ?? "transfer_error", response.status, err.details ?? {});
  }
  return new XferkitError(`unexpected HTTP ${response.status}`, "transfer_error", response.status);
}

/**
 * fetch with retries on network errors and retryable statuses (honouring
 * Retry-After). Returns the final response; error statuses are left to the caller.
 */
export async function fetchWithRetry(
  conn: ConnectionOptions,
  url: string,
  init: RequestInit,
  extraRetryStatuses: number[] = [],
): Promise<Response> {
  const retry = retryDefaults(conn.retry);
  const doFetch = conn.fetch ?? globalThis.fetch.bind(globalThis);
  for (let attempt = 0; ; attempt++) {
    throwIfAborted(conn.signal);
    const last = attempt >= retry.attempts - 1;
    let response: Response;
    try {
      response = await doFetch(url, { credentials: conn.credentials, ...init, signal: conn.signal });
    } catch (error) {
      if (conn.signal?.aborted || last) throw error;
      await sleep(backoff(attempt, retry, null), conn.signal);
      continue;
    }
    if (!last && (RETRYABLE.has(response.status) || extraRetryStatuses.includes(response.status))) {
      await response.body?.cancel().catch(() => undefined);
      await sleep(backoff(attempt, retry, response.headers.get("Retry-After")), conn.signal);
      continue;
    }
    return response;
  }
}

export async function fetchJson<T>(
  conn: ConnectionOptions,
  url: string,
  init: RequestInit,
  extraRetryStatuses: number[] = [],
): Promise<{ data: T; response: Response }> {
  const response = await fetchWithRetry(conn, url, init, extraRetryStatuses);
  if (!response.ok) throw await errorFromResponse(response);
  return { data: (await response.json()) as T, response };
}

// ------------------------------------------------------------------ hashing & encoding

export async function sha256(data: ArrayBuffer | Uint8Array): Promise<Uint8Array> {
  return new Uint8Array(await crypto.subtle.digest("SHA-256", data as BufferSource));
}

export function toHex(bytes: Uint8Array): string {
  let out = "";
  for (const b of bytes) out += b.toString(16).padStart(2, "0");
  return out;
}

export function fromHex(hex: string): Uint8Array {
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i++) out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  return out;
}

export function base64url(bytes: Uint8Array): string {
  let binary = "";
  for (const b of bytes) binary += String.fromCharCode(b);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

export function randomToken(bytes = 18): string {
  return base64url(crypto.getRandomValues(new Uint8Array(bytes)));
}

/** Same derivation as the Python client, so a key means the same upload in both. */
export async function tokenForKey(key: string): Promise<string> {
  const prefix = new TextEncoder().encode("xferkit-idempotency\0");
  const body = new TextEncoder().encode(key);
  const joined = new Uint8Array(prefix.length + body.length);
  joined.set(prefix);
  joined.set(body, prefix.length);
  return base64url(await sha256(joined));
}

export async function compositeChecksum(digests: Uint8Array[]): Promise<string> {
  const joined = new Uint8Array(digests.length * 32);
  digests.forEach((d, i) => joined.set(d, i * 32));
  return `sha256=${toHex(await sha256(joined))}`;
}

/** Run `worker` over `items` with at most `concurrency` in flight; the first
 * error stops the others (they finish their current item). */
export async function runPool<T>(items: T[], concurrency: number, worker: (item: T) => Promise<void>): Promise<void> {
  const queue = items.slice();
  let failure: unknown = null;
  const runners = Array.from({ length: Math.min(concurrency, queue.length) }, async () => {
    while (queue.length && failure === null) {
      const item = queue.shift() as T;
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
