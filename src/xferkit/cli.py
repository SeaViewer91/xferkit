"""Command line interface: ``xferkit upload`` / ``xferkit download``.

Requires the client extra: ``pip install "xferkit[client]"``.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from ._version import __version__
from .protocol import MiB, parse_size

DEFAULT_STATE_DIR = Path("~/.cache/xferkit").expanduser()


class _ProgressPrinter:
    def __init__(self, label: str, stream=sys.stderr) -> None:
        self.label = label
        self.stream = stream
        self.started = time.monotonic()
        self._last = 0.0
        self._first_done: int | None = None

    def __call__(self, progress) -> None:  # TransferProgress
        now = time.monotonic()
        if self._first_done is None:
            self._first_done = progress.bytes_done
        if now - self._last < 0.2 and progress.bytes_done < progress.bytes_total:
            return
        self._last = now
        elapsed = max(now - self.started, 1e-6)
        speed = (progress.bytes_done - self._first_done) / elapsed / MiB
        files = ""
        if hasattr(progress, "files_total"):
            files = f"{progress.files_done + progress.files_failed}/{progress.files_total} files  "
        self.stream.write(
            f"\r{self.label}: {progress.fraction * 100:6.2f}%  {files}"
            f"{progress.bytes_done / MiB:,.1f}/{progress.bytes_total / MiB:,.1f} MiB  "
            f"{speed:,.1f} MiB/s "
        )
        self.stream.flush()

    def finish(self) -> None:
        self.stream.write("\n")
        self.stream.flush()


def _headers(values: list[str]) -> dict[str, str]:
    headers = {}
    for value in values:
        name, sep, content = value.partition(":")
        if not sep or not name.strip():
            raise SystemExit(f"invalid header {value!r}; expected 'Name: value'")
        headers[name.strip()] = content.strip()
    return headers


def _metadata(values: list[str]) -> dict[str, str]:
    metadata = {}
    for value in values:
        key, sep, content = value.partition("=")
        if not sep or not key:
            raise SystemExit(f"invalid metadata {value!r}; expected 'key=value'")
        metadata[key] = content
    return metadata


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="xferkit", description="Resumable large-file upload/download."
    )
    parser.add_argument("--version", action="version", version=f"xferkit {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    up = sub.add_parser("upload", help="upload a file or a folder to an xferkit server")
    up.add_argument("file", type=Path, help="file, or folder to upload with its sub-folders")
    up.add_argument("url", help="upload endpoint, e.g. https://host/uploads")
    up.add_argument("--filename", help="name to store on the server")
    up.add_argument("--chunk-size", type=parse_size, help="part size, e.g. 8M (server default)")
    up.add_argument("-c", "--concurrency", type=int, default=4)
    up.add_argument("--meta", action="append", default=[], metavar="KEY=VALUE")
    up.add_argument("-H", "--header", action="append", default=[], metavar="'NAME: VALUE'")
    up.add_argument("--grant", help="signed grant token issued by the server's application")
    up.add_argument("--upload-id", help="resume this upload session")
    up.add_argument(
        "--idempotency-key", help="re-running with the same key never uploads a second copy"
    )
    up.add_argument("--state-dir", type=Path, default=DEFAULT_STATE_DIR)
    folder = up.add_argument_group("folders")
    folder.add_argument("--include", action="append", metavar="GLOB", help="e.g. '*.jpg'")
    folder.add_argument("--exclude", action="append", metavar="GLOB")
    folder.add_argument("--hidden", action="store_true", help="include dot files and folders")
    folder.add_argument(
        "--files", type=int, metavar="N", help="files in progress at once (default max(c, 8))"
    )
    folder.add_argument(
        "--key-prefix",
        help="build re-run detection keys from this prefix and relative paths "
        "instead of absolute paths (same folder elsewhere = same uploads)",
    )
    up.add_argument("--checksum", action="store_true", help="also verify a whole-file sha256")
    up.add_argument("-q", "--quiet", action="store_true")

    down = sub.add_parser("download", help="download a URL with parallel range requests")
    down.add_argument("url")
    down.add_argument("dest", type=Path)
    down.add_argument("-c", "--concurrency", type=int, default=4)
    down.add_argument("--segment-size", type=parse_size, default=16 * MiB)
    down.add_argument("-H", "--header", action="append", default=[], metavar="'NAME: VALUE'")
    down.add_argument("--grant", help="signed grant token (or put it in the URL as ?xk=...)")
    down.add_argument("--checksum", help="expected sha256=<hex>")
    down.add_argument("--overwrite", action="store_true")
    down.add_argument("--no-resume", action="store_true")
    down.add_argument("-q", "--quiet", action="store_true")
    return parser


def _upload_folder(args: argparse.Namespace, printer, upload_folder) -> int:
    for option in ("filename", "upload_id", "idempotency_key"):
        if getattr(args, option):
            raise SystemExit(f"--{option.replace('_', '-')} cannot be used with a folder")
    result = upload_folder(
        args.url,
        args.file,
        include=args.include,
        exclude=args.exclude,
        include_hidden=args.hidden,
        key_prefix=args.key_prefix,
        file_concurrency=args.files,
        metadata=_metadata(args.meta),
        progress=printer,
        headers=_headers(args.header),
        grant=args.grant,
        chunk_size=args.chunk_size,
        concurrency=args.concurrency,
        send_checksum=args.checksum,
    )
    if printer:
        printer.finish()
    for failure in result.failed:
        print(f"failed: {failure.path}: {failure.error}", file=sys.stderr)
    skipped = sum(r.resumed for r in result.uploaded)
    print(
        f"{len(result.uploaded)} uploaded ({skipped} already on the server or resumed), "
        f"{len(result.failed)} failed"
    )
    return 0 if result.ok else 1


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        from .client import download_file, upload_file, upload_folder
    except ImportError:  # pragma: no cover
        print('the client extra is required: pip install "xferkit[client]"', file=sys.stderr)
        return 2
    from .errors import XferkitError

    printer = None if args.quiet else _ProgressPrinter(args.command)
    try:
        if args.command == "upload" and args.file.is_dir():
            return _upload_folder(args, printer, upload_folder)
        if args.command == "upload":
            result = upload_file(
                args.url,
                args.file,
                filename=args.filename,
                metadata=_metadata(args.meta),
                upload_id=args.upload_id,
                idempotency_key=args.idempotency_key,
                progress=printer,
                headers=_headers(args.header),
                grant=args.grant,
                chunk_size=args.chunk_size,
                concurrency=args.concurrency,
                state_dir=args.state_dir,
                send_checksum=args.checksum,
            )
            if printer:
                printer.finish()
            print(f"upload_id={result.upload_id} size={result.size} {result.composite_checksum}")
        else:
            result = download_file(
                args.url,
                args.dest,
                overwrite=args.overwrite,
                resume=not args.no_resume,
                checksum=args.checksum,
                progress=printer,
                headers=_headers(args.header),
                grant=args.grant,
                concurrency=args.concurrency,
                segment_size=args.segment_size,
            )
            if printer:
                printer.finish()
            print(
                f"saved {result.path} ({result.size} bytes{', resumed' if result.resumed else ''})"
            )
    except KeyboardInterrupt:
        print("\ninterrupted — run the same command again to resume", file=sys.stderr)
        return 130
    except (XferkitError, OSError) as exc:
        if printer:
            printer.finish()
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
