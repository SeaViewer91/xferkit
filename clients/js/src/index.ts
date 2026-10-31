/**
 * xferkit browser client: parallel, resumable, verified uploads and downloads
 * against an xferkit server.
 *
 * ```js
 * import { upload, download } from "xferkit";
 *
 * const result = await upload(fileInput.files[0], {
 *   baseUrl: "https://files.example.com/uploads",
 *   grant,                      // signed grant from your application (optional)
 *   onProgress: (p) => bar.value = p.fraction,
 * });
 *
 * // A whole folder (<input type="file" webkitdirectory> or a drop):
 * const batch = await uploadFiles(folderInput.files, { baseUrl, grant });
 * ```
 */

export {
  CHECKSUM_HEADER,
  GRANT_HEADER,
  PROTOCOL_HEADER,
  XferkitError,
  type ConnectionOptions,
  type ProgressCallback,
  type RetryOptions,
  type TransferProgress,
} from "./common";
export {
  Uploader,
  abortUpload,
  getUploadStatus,
  upload,
  type ResumeStore,
  type UploadOptions,
  type UploadResult,
  type UploadState,
} from "./upload";
export {
  filesFromDataTransfer,
  uploadFiles,
  type BatchProgress,
  type BatchResult,
  type FileEntry,
  type FileFailed,
  type FileUploaded,
  type UploadFilesOptions,
} from "./batch";
export {
  download,
  saveUrl,
  type DownloadOptions,
  type PositionalWriter,
  type WritableFileHandle,
} from "./download";

export const VERSION = "0.1.0";
