# Benchmarks

- [load/](load/README.md) — thousands of ~10 MB images, 10 concurrent client
  sessions, a multi-worker server and fault injection. Results:
  [load/RESULTS.md](load/RESULTS.md).
- [small_files.py](small_files.py) — many small files: one `upload()` per file
  vs. `upload_many()` (batch session creation + server-side auto-completion).

## Small files (2026-10-31)

```
python benchmarks/small_files.py --files 2000 --size 64K --concurrency 16 --server-workers 2
```

2000 files × 64 KiB (125 MiB), 16 files in flight, client and a 2-worker
server on the same 2-core machine, loopback:

| mode | seconds | files/s | MiB/s | requests/file |
|---|---:|---:|---:|---:|
| `upload()` per file | 17.3 | 116 | 7.2 | 3.00 |
| `upload_many()` | 11.7 | 171 | 10.7 | 1.01 |
| `upload_many()` again (all files already uploaded) | 1.9 | 1,032 | 64.5 | 0.01 |

A small file costs one request instead of three (create, part, complete): the
sessions are created 100 per request and the server completes each file when
its only part arrives. Over a real network, where each request costs a round
trip, the saving is larger than on loopback. Re-running a finished batch only
hashes the local files and compares them with the server's records (20
requests for 2000 files).

On this machine the remaining time is mostly server CPU and file-system work
per file (session directory, part marker, rename into place), shared with the
client on two cores.
