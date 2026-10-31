// Many files at once: folders, multi-select, drag and drop.

import {
  type TransferProgress,
  XferkitError,
  buildHeaders,
  errorFromResponse,
  fetchJson,
  fetchWithRetry,
  randomToken,
  throwIfAborted,
  tokenForKey,
} from "./common";
import { Limiter, type UploadOptions, type UploadResult, type UploadState, join, transfer } from "./upload";

/** A file plus where it came from. */
export interface FileEntry {
  file: Blob;
  /** Path inside the chosen folder, "/"-separated (default: from
   * `webkitRelativePath`, else the file name). Sent as `metadata.relative_path`. */
  relativePath?: string;
  /** Name of the folder the user picked or dropped ("" for loose files). */
  folder?: string;
  /** Name stored on the server (default: the file's name). */
  filename?: string;
  metadata?: Record<string, string>;
  /** Overrides the derived idempotency key for this file. */
  idempotencyKey?: string;
}

export interface BatchProgress extends TransferProgress {
  filesDone: number;
  filesFailed: number;
  filesTotal: number;
}

export interface FileUploaded extends UploadResult {
  relativePath: string;
}

export interface FileFailed {
  file: Blob;
  relativePath: string;
  error: unknown;
}

export interface BatchResult {
  uploaded: FileUploaded[];
  failed: FileFailed[];
  ok: boolean;
}

export interface UploadFilesOptions
  extends Omit<UploadOptions, "filename" | "idempotencyKey" | "uploadId" | "resume" | "onProgress"> {
  /** Derive each file's idempotency key from its path, name, size and
   * modification time, so running the same batch again resumes unfinished
   * files and skips (after verifying) finished ones (default true). */
  dedupe?: boolean;
  /** Prefix of the derived keys (default: the folder name + "/"). The Python
   * client's `upload_folder(root, key_prefix="photos/")` derives the same keys
   * as this client uploading a folder named "photos". */
  keyPrefix?: string;
  /** Files in progress at once (default 6). Parts in flight across all files
   * stay limited to `concurrency`. */
  fileConcurrency?: number;
  /** Files per session-creation request (default 100). */
  batchSize?: number;
  /** Largest file the server completes by itself when its last part arrives
   * (default 64 MiB), saving one request per file. */
  autoCompleteBelow?: number;
  /** Skip files and folders whose name starts with "." inside picked folders (default true). */
  skipHidden?: boolean;
  onProgress?: (progress: BatchProgress) => void;
  /** Called as soon as each file is done or has failed. */
  onFile?: (outcome: FileUploaded | FileFailed) => void;
}

interface Planned {
  index: number;
  file: Blob;
  filename: string;
  relativePath: string;
  metadata: Record<string, string>;
  token: string;
}

type Created = { status: UploadState; created: boolean } | { error: unknown };

const batchSupport = new Map<string, boolean>();

function splitPath(entry: FileEntry): { folder: string; relativePath: string } {
  if (entry.relativePath !== undefined) return { folder: entry.folder ?? "", relativePath: entry.relativePath };
  const webkit = (entry.file as File).webkitRelativePath;
  if (webkit) {
    const slash = webkit.indexOf("/");
    return { folder: webkit.slice(0, slash), relativePath: webkit.slice(slash + 1) };
  }
  return { folder: entry.folder ?? "", relativePath: (entry.file as File).name ?? "blob" };
}

/** Same derivation as the Python client's dedupe keys. */
function dedupeKey(base: string, filename: string, file: Blob): string {
  const modified = Math.floor((file as File).lastModified ?? 0);
  return `xferkit-file\n${base}\n${filename}\n${file.size}\n${modified}`;
}

function isFileEntry(value: Blob | FileEntry): value is FileEntry {
  return !(value instanceof Blob);
}

/**
 * Upload many files (a picked or dropped folder, a multi-selection). Files
 * succeed or fail independently; the result lists both.
 *
 * Folder structure is kept as `metadata.relative_path`. Sessions are created
 * `batchSize` at a time with one request (`POST {baseUrl}/batch`, with a
 * fallback for servers without it), and small files are completed by the
 * server on their last part.
 */
export async function uploadFiles(files: Iterable<Blob | FileEntry> | ArrayLike<Blob | FileEntry>, options: UploadFilesOptions): Promise<BatchResult> {
  const baseUrl = options.baseUrl.replace(/\/+$/, "");
  const dedupe = options.dedupe ?? true;
  const filesAtOnce = Math.max(1, options.fileConcurrency ?? 6);
  const batchSize = Math.max(1, options.batchSize ?? 100);
  const autoCompleteBelow = options.autoCompleteBelow ?? 64 * 1024 * 1024;
  const skipHidden = options.skipHidden ?? true;

  const planned: Planned[] = [];
  for (const raw of Array.from(files as ArrayLike<Blob | FileEntry>)) {
    const entry: FileEntry = isFileEntry(raw) ? raw : { file: raw };
    const { folder, relativePath } = splitPath(entry);
    if (skipHidden && folder && relativePath.split("/").some((part) => part.startsWith("."))) continue;
    const filename = entry.filename ?? (entry.file as File).name ?? relativePath.split("/").pop() ?? "blob";
    let token: string;
    if (entry.idempotencyKey !== undefined) token = await tokenForKey(entry.idempotencyKey);
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
  const report = () =>
    options.onProgress?.({
      bytesDone,
      bytesTotal: total,
      fraction: total === 0 ? 1 : bytesDone / total,
      filesDone,
      filesFailed,
      filesTotal: planned.length,
    });

  const uploaded = new Map<number, FileUploaded>();
  const failed = new Map<number, FileFailed>();
  const fail = (item: Planned, error: unknown, unsent: number) => {
    const outcome: FileFailed = { file: item.file, relativePath: item.relativePath, error };
    failed.set(item.index, outcome);
    filesFailed++;
    bytesDone += unsent;
    report();
    options.onFile?.(outcome);
  };

  const limiter = new Limiter(Math.max(1, options.concurrency ?? 4));
  const transferOptions: UploadOptions = { ...options, onProgress: undefined };
  const pending = planned.slice();
  const ready: { item: Planned; status: UploadState; created: boolean }[] = [];
  const running = new Set<Promise<void>>();
  let throttled = false; // the server refused new sessions; wait for one of ours to finish
  let backoff = 0;
  const attempts = options.retry?.attempts ?? 6;

  const start = (item: Planned, status: UploadState, created: boolean) => {
    let sent = 0;
    const task: Promise<void> = (async () => {
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
          limiter,
        );
        const outcome: FileUploaded = { ...result, relativePath: item.relativePath };
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
    // Registered before anyone races on the task, so it runs first.
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
        const retryLater: Planned[] = [];
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
            // Other uploads fill the server's limit: back off, then give up on these.
            if (backoff >= attempts - 1) {
              const error = new XferkitError("the server accepts no more uploads now", "too_many_uploads", 429);
              for (const item of pending.splice(0)) fail(item, error, item.file.size);
              continue;
            }
            await new Promise((r) => setTimeout(r, Math.min(30_000, 500 * 2 ** backoff)));
            backoff++;
            continue;
          }
        } else backoff = 0;
      }
      while (ready.length && running.size < filesAtOnce) {
        const next = ready.shift()!;
        start(next.item, next.status, next.created);
      }
      if (running.size) await Promise.race(running);
    }
  } catch (error) {
    await Promise.allSettled(Array.from(running));
    throw error;
  }

  const byIndex = <T>(map: Map<number, T>) => Array.from(map.keys()).sort((a, b) => a - b).map((k) => map.get(k)!);
  return { uploaded: byIndex(uploaded), failed: byIndex(failed), ok: failed.size === 0 };
}

async function createMany(baseUrl: string, items: Planned[], autoCompleteBelow: number, options: UploadFilesOptions): Promise<Created[]> {
  const bodies = items.map((item) => {
    const body: Record<string, unknown> = {
      filename: item.filename,
      size: item.file.size,
      metadata: item.metadata,
      client_token: item.token,
    };
    if (options.chunkSize !== undefined) body.chunk_size = options.chunkSize;
    if (item.file.size <= autoCompleteBelow) body.auto_complete = true;
    return body;
  });
  const headers = buildHeaders(options, { "Content-Type": "application/json" });

  if (batchSupport.get(baseUrl) !== false) {
    let response: Response;
    try {
      response = await fetchWithRetry(options, join(baseUrl, "batch"), {
        method: "POST",
        headers,
        body: JSON.stringify({ files: bodies }),
      });
    } catch (error) {
      if (options.signal?.aborted) throw error;
      return items.map(() => ({ error }));
    }
    if ((response.status === 404 || response.status === 405) && !batchSupport.has(baseUrl)) {
      batchSupport.set(baseUrl, false); // an older or minimal server
      await response.body?.cancel().catch(() => undefined);
    } else if (!response.ok) {
      const error = await errorFromResponse(response);
      return items.map(() => ({ error }));
    } else {
      batchSupport.set(baseUrl, true);
      const { uploads } = (await response.json()) as { uploads: Record<string, unknown>[] };
      return uploads.map((entry) => {
        const error = entry.error as { code?: string; message?: string; details?: Record<string, unknown> } | undefined;
        if (error) {
          return {
            error: new XferkitError(error.message ?? "failed", error.code ?? "transfer_error", Number(entry.status ?? 0), error.details ?? {}),
          };
        }
        const { created, ...status } = entry;
        return { status: status as UploadState, created: Boolean(created) };
      });
    }
  }

  return Promise.all(
    bodies.map(async (body): Promise<Created> => {
      try {
        const { data, response } = await fetchJson<UploadState>(options, baseUrl, {
          method: "POST",
          headers,
          body: JSON.stringify(body),
        });
        return { status: data, created: response.status === 201 };
      } catch (error) {
        if (options.signal?.aborted) throw error;
        return { error };
      }
    }),
  );
}

/**
 * Files from a drop event, including the contents of dropped folders (call it
 * synchronously inside the `drop` handler, before any `await`).
 */
export function filesFromDataTransfer(dataTransfer: DataTransfer): Promise<FileEntry[]> {
  // DataTransfer items are only readable during the event, so take the entries now.
  const roots: FileSystemEntry[] = [];
  const loose: File[] = [];
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
    const out: FileEntry[] = loose.map((file) => ({ file }));
    for (const root of roots) {
      if (root.isFile) out.push({ file: await entryFile(root as FileSystemFileEntry) });
      else await walk(root as FileSystemDirectoryEntry, root.name, "", out);
    }
    return out;
  })();
}

function entryFile(entry: FileSystemFileEntry): Promise<File> {
  return new Promise((resolve, reject) => entry.file(resolve, reject));
}

async function walk(directory: FileSystemDirectoryEntry, folder: string, prefix: string, out: FileEntry[]): Promise<void> {
  const reader = directory.createReader();
  const children: FileSystemEntry[] = [];
  // readEntries returns the listing in pieces (100 at a time in Chromium).
  for (;;) {
    const batch = await new Promise<FileSystemEntry[]>((resolve, reject) => reader.readEntries(resolve, reject));
    if (!batch.length) break;
    children.push(...batch);
  }
  children.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
  for (const child of children) {
    const relativePath = prefix + child.name;
    if (child.isFile) out.push({ file: await entryFile(child as FileSystemFileEntry), folder, relativePath });
    else await walk(child as FileSystemDirectoryEntry, folder, `${relativePath}/`, out);
  }
}
