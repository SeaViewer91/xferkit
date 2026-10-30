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
        self.stream.write(
            f"\r{self.label}: {progress.fraction * 100:6.2f}%  "
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

    up = sub.add_parser("upload", help="upload a file to an xferkit server")
    up.add_argument("file", type=Path)
    up.add_argument("url", help="upload endpoint, e.g. https://host/uploads")
    up.add_argument("--filename", help="name to store on the server")
    up.add_argument("--chunk-size", type=parse_size, help="part size, e.g. 8M (server default)")
    up.add_argument("-c", "--concurrency", type=int, default=4)
    up.add_argument("--meta", action="append", default=[], metavar="KEY=VALUE")
    up.add_argument("-H", "--header", action="append", default=[], metavar="'NAME: VALUE'")
    up.add_argument("--upload-id", help="resume this upload session")
    up.add_argument("--state-dir", type=Path, default=DEFAULT_STATE_DIR)
    up.add_argument("--checksum", action="store_true", help="also verify a whole-file sha256")
    up.add_argument("-q", "--quiet", action="store_true")

    down = sub.add_parser("download", help="download a URL with parallel range requests")
    down.add_argument("url")
    down.add_argument("dest", type=Path)
    down.add_argument("-c", "--concurrency", type=int, default=4)
    down.add_argument("--segment-size", type=parse_size, default=16 * MiB)
    down.add_argument("-H", "--header", action="append", default=[], metavar="'NAME: VALUE'")
    down.add_argument("--checksum", help="expected sha256=<hex>")
    down.add_argument("--overwrite", action="store_true")
    down.add_argument("--no-resume", action="store_true")
    down.add_argument("-q", "--quiet", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        from .client import download_file, upload_file
    except ImportError:  # pragma: no cover
        print('the client extra is required: pip install "xferkit[client]"', file=sys.stderr)
        return 2
    from .errors import XferkitError

    printer = None if args.quiet else _ProgressPrinter(args.command)
    try:
        if args.command == "upload":
            result = upload_file(
                args.url,
                args.file,
                filename=args.filename,
                metadata=_metadata(args.meta),
                upload_id=args.upload_id,
                progress=printer,
                headers=_headers(args.header),
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
