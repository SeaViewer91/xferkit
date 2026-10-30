# Load test

Latest results: [RESULTS.md](RESULTS.md).

Uploads and downloads thousands of synthetic ~10 MB drone-style images through a
multi-worker xferkit server, with many concurrent client sessions and injected
faults, and verifies every byte.

## Run

```bash
pip install -e ".[bench]"
python benchmarks/load/run_load_test.py --images 800 --sessions 10 --server-workers 4
```

The first run generates the images (about 0.35 s each on 2 cores) into
`loadtest-data/images` and reuses them afterwards. Results are written to
`loadtest-data/results/<timestamp>/report.md` and `report.json`.

Disk: the images plus one server-side copy (about 2.1× the image set); downloaded
files are deleted as soon as they are verified. 800 images ≈ 7.7 GiB → ~18 GiB free.

| Option | Default | Meaning |
|---|---|---|
| `--images` | 800 | number of ~10 MB images |
| `--sessions` | 10 | concurrent client sessions (separate processes) |
| `--server-workers` | 4 | uvicorn worker processes sharing one store |
| `--file-concurrency` | 4 | files in flight per session |
| `--part-concurrency` | 2 | parts / range segments in flight per file |
| `--segment-size` | 4 MiB | download range size |
| `--scenarios` | `S0,…,S5` | subset to run |
| `--keep` | off | keep each scenario's server store |

## Scenarios

| | What it does |
|---|---|
| S0 | smoke test: 1 session, 50 images, upload + download |
| S1 | baseline: 1 session, all images |
| S2 | all sessions upload at once, then all download at once |
| S3 | half the sessions upload new images while the other half download |
| S4 | 3 sessions SIGKILLed at 25 %, whole server SIGKILLed and restarted at 50 %, killed sessions resumed |
| S5 | 1 / 4 / 8 files in flight per session (200-image subset) |

## Checks (a scenario fails if any is violated)

- every file uploaded and downloaded (after retries)
- every stored file matches the manifest SHA-256 (server side) and every download is verified (client side)
- no leftover pending sessions, `.lock`, `.tmp` or `.completing` files in the store
- no completed upload the client did not acknowledge (duplicate copies)

## On macOS

The default open-file limit (256) is too low for 10 sessions × several files.
Raise it in the shell first: `ulimit -n 10240`.
