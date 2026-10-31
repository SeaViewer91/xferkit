// Parallel ranged downloads into memory (Blob) or straight to disk.

import {
  type ConnectionOptions,
  type ProgressCallback,
  XferkitError,
  buildHeaders,
  errorFromResponse,
  fetchWithRetry,
  progressOf,
  runPool,
  sha256,
  throwIfAborted,
  toHex,
} from "./common";

/** Anything with positional writes, e.g. FileSystemWritableFileStream. */
export interface PositionalWriter {
  write(chunk: { type: "write"; position: number; data: BufferSource }): Promise<void>;
  close(): Promise<void>;
  abort?(reason?: unknown): Promise<void>;
}

/** A file picked with the File System Access API (`showSaveFilePicker`). */
export interface WritableFileHandle {
  createWritable(options?: { keepExistingData?: boolean }): Promise<PositionalWriter>;
}

export interface DownloadOptions extends ConnectionOptions {
  /** Segments downloaded at once (default 4). */
  concurrency?: number;
  /** Bytes per range request (default 8 MiB). */
  segmentSize?: number;
  /** Write to this file instead of returning a Blob. Use it for files too large
   * to hold in memory (Chromium-based browsers: `showSaveFilePicker()`). */
  target?: WritableFileHandle | PositionalWriter;
  /** Expected "sha256=<hex>" of the whole file (Blob mode only). */
  checksum?: string;
  onProgress?: ProgressCallback;
}

interface Probe {
  size: number | null;
  ranged: boolean;
  validator: string | null;
  contentType: string;
}

class RemoteChanged extends XferkitError {
  constructor() {
    super("the remote file changed during the download", "remote_changed", 412);
  }
}

const CONTENT_RANGE = /^\s*bytes\s+(?:(\d+)-(\d+)|\*)\/(\d+|\*)\s*$/i;

function parseContentRange(value: string | null): [number | null, number | null, number | null] {
  const m = CONTENT_RANGE.exec(value ?? "");
  if (!m) return [null, null, null];
  return [m[1] ? Number(m[1]) : null, m[2] ? Number(m[2]) : null, m[3] && m[3] !== "*" ? Number(m[3]) : null];
}

async function probe(url: string, options: DownloadOptions): Promise<Probe> {
  const response = await fetchWithRetry(options, url, {
    method: "GET",
    headers: buildHeaders(options, { Range: "bytes=0-0" }),
  });
  const etag = response.headers.get("ETag");
  const lastModified = response.headers.get("Last-Modified");
  const validator = etag && !etag.startsWith("W/") ? etag : lastModified;
  const contentType = response.headers.get("Content-Type") ?? "application/octet-stream";
  await response.body?.cancel().catch(() => undefined);
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

async function openWriter(target: DownloadOptions["target"]): Promise<PositionalWriter | null> {
  if (!target) return null;
  if ("createWritable" in target) return target.createWritable({ keepExistingData: false });
  return target;
}

/**
 * Download `url` with parallel range requests.
 *
 * Returns a Blob, or `undefined` when `options.target` is given (the data was
 * written there). Works with any server that supports HTTP ranges. For very
 * large files without the File System Access API, prefer {@link saveUrl},
 * which hands a (signed) URL to the browser's own download manager.
 */
export async function download(url: string, options: DownloadOptions = {}): Promise<Blob | undefined> {
  try {
    return await downloadOnce(url, options);
  } catch (error) {
    if (error instanceof RemoteChanged) return downloadOnce(url, options); // start over once
    throw error;
  }
}

async function downloadOnce(url: string, options: DownloadOptions): Promise<Blob | undefined> {
  const info = await probe(url, options);
  const writer = await openWriter(options.target);
  try {
    let blob: Blob;
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
          actual,
        });
      }
    }
    if (writer) {
      await writer.close();
      return undefined;
    }
    return blob;
  } catch (error) {
    await writer?.abort?.(error).catch(() => undefined);
    throw error;
  }
}

async function downloadWhole(url: string, options: DownloadOptions, info: Probe): Promise<Blob> {
  const response = await fetchWithRetry(options, url, { method: "GET", headers: buildHeaders(options) });
  if (response.status !== 200) throw await errorFromResponse(response);
  const total = info.size ?? Number(response.headers.get("Content-Length") ?? 0);
  if (!response.body) return response.blob();
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let done = 0;
  for (;;) {
    const { value, done: finished } = await reader.read();
    if (finished) break;
    chunks.push(value);
    done += value.byteLength;
    options.onProgress?.(progressOf(done, total));
  }
  return new Blob(chunks as BlobPart[], { type: info.contentType });
}

async function downloadRanged(
  url: string,
  options: DownloadOptions,
  info: Probe,
  writer: PositionalWriter | null,
): Promise<BlobPart[]> {
  const size = info.size!;
  const step = Math.max(1, options.segmentSize ?? 8 * 1024 * 1024);
  const segments: Array<[number, number, number]> = [];
  for (let i = 0, start = 0; start < size; i++, start += step) {
    segments.push([i, start, Math.min(start + step, size) - 1]);
  }
  const parts: BlobPart[] = new Array(segments.length);
  let done = 0;
  options.onProgress?.(progressOf(0, size));

  await runPool(segments, Math.max(1, options.concurrency ?? 4), async ([index, start, end]) => {
    throwIfAborted(options.signal);
    const headers: Record<string, string> = { Range: `bytes=${start}-${end}` };
    if (info.validator) headers["If-Range"] = info.validator;
    for (let attempt = 0; ; attempt++) {
      const response = await fetchWithRetry(options, url, { method: "GET", headers: buildHeaders(options, headers) });
      if (response.status === 200) {
        await response.body?.cancel().catch(() => undefined);
        throw new RemoteChanged();
      }
      if (response.status !== 206) throw await errorFromResponse(response);
      const [from, to, total] = parseContentRange(response.headers.get("Content-Range"));
      if (total !== null && total !== size) throw new RemoteChanged();
      if (from !== start || to !== end) {
        throw new XferkitError(`server sent range ${from}-${to}, expected ${start}-${end}`, "transfer_error");
      }
      let data: ArrayBuffer;
      try {
        data = await response.arrayBuffer();
      } catch (error) {
        if (options.signal?.aborted || attempt >= 4) throw error;
        continue; // connection dropped mid-body: fetch the segment again
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

/**
 * Let the browser download `url` itself (its download manager can pause and
 * resume, and nothing is held in page memory). Combine with a signed URL from
 * your application, since the browser cannot add custom headers.
 */
export function saveUrl(url: string, filename?: string): void {
  const link = document.createElement("a");
  link.href = url;
  if (filename) link.download = filename;
  link.rel = "noopener";
  link.style.display = "none";
  document.body.appendChild(link);
  link.click();
  link.remove();
}
