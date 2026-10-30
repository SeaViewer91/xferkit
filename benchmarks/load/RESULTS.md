# Load test results — 2026-10-31

Run with `run_load_test.py` on the final 0.1.0 code.

## Environment

| | |
|---|---|
| Machine | Linux x86_64 VM, **2 CPU cores**, 7.8 GiB RAM, local disk |
| Software | Python 3.13, FastAPI 0.162, uvicorn 0.54, httpx 0.28 |
| Data | 800 synthetic JPEG images, 5472×3648 (20 MP), 8.8–10.7 MiB each (avg 9.9 MiB), **7.7 GiB** total |
| Server | uvicorn, **4 worker processes** sharing one `LocalStorage` directory |
| Clients | **10 sessions**, each a separate OS process; 4 files × 2 parts in flight per session |

Server and all clients ran on the same 2-core machine over loopback, so the
throughput figures are CPU-bound and only indicative. Correctness and
concurrency results do not depend on the hardware.

## Results (all scenarios passed)

| Scenario | Phase | Files | MiB/s | p50 / p95 / p99 s per file | Retries |
|---|---|---|---|---|---|
| S0 smoke | upload / download | 50 / 50 | 124 / 151 | 0.27 / 0.35 / 0.37 | 0 |
| S1 1 session | upload | 800 / 800 | 129 | 0.28 / 0.45 / 0.70 | 0 |
| | download | 800 / 800 | 160 | 0.24 / 0.31 / 0.34 | 0 |
| S2 10 sessions | upload | 800 / 800 | 161 | 2.29 / 3.38 / 3.84 | 0 |
| | download | 800 / 800 | 178 | 2.09 / 2.93 / 3.09 | 0 |
| S3 mixed | 5 upload + 5 download sessions at once | 400 + 400 | 168 combined | up 1.52 / 2.22 / 2.46 · down 1.26 / 5.40 / 6.39 | 0 |
| S4 faults | upload with faults | 800 / 800 | 138 | 2.04 / 3.58 / 7.92 | 148 |
| | download afterwards | 800 / 800 | 180 | 2.13 / 2.75 / 3.05 | 0 |
| S5 sweep | 1 / 4 / 8 files in flight per session | 200 each | 154 / 150 / 129 | p50 0.55 / 2.04 / 4.42 | 0 / 0 / 2 |

Every scenario also verified, on the server, that every stored file matches the
SHA-256 of its source image (800/800), and on the client that every download
matches before it is kept. No stored file ever failed verification.

**S4 timeline:** at 25 % (237/800 files) three client sessions were SIGKILLed;
at 50 % (401/800) the whole server (master + 4 workers) was SIGKILLed and
restarted 5 s later on the same storage; the killed sessions were restarted
with `--resume`. All 800 files arrived exactly once; 10 files were finished by
resuming, 148 requests were retried automatically while the server was down.

**Repeated fault injection:** S4 was run 10 more times (120 images, 6 sessions)
to hit different crash timings. 10/10 passed: no failed file, no duplicate, no
leftover lock or unfinished session. Server crashes left crash debris
(half-removed session directories) in 3 of the 10 runs (4 directories in total);
all of it was removed by garbage collection.

**Resources:** server RSS for all 6 processes peaked at 477 MiB (S2), rising
once per phase (warm-up) and flat within phases — no growth pattern that would
indicate a leak. Peak open files on the server: 199. Each client session used
about 270 MiB with 4 files × 2 parts × 8 MiB in flight.

**Concurrency sweep (S5):** on 2 cores, 1–4 files in flight per session already
saturate the machine; 8 only adds latency (and 2 transient connection retries
that recovered automatically). Re-run the sweep on the target server to pick a
value.

## Defects found and fixed

The first runs on the original code failed S4 in **8 out of 8** repetitions.
Five defects were found, all in crash/recovery paths, and fixed with
regression tests:

| # | Symptom under test | Cause | Fix |
|---|---|---|---|
| 1 | Files failed after a server restart (409 until retries ran out) | Part locks of killed workers were honoured for 10 min | Locks record host + pid; a lock whose process is gone is reclaimed at once |
| 2 | Same file stored twice | Client crashed after the server completed the upload; on re-run the completed id was "not found" | Status endpoint answers for completed uploads (`state: "completed"`); client verifies instead of re-uploading |
| 3 | Orphaned sessions | Client crashed after creating a session, before recording its id | Idempotent creation with a client token journaled before the request; server binds the token before creating the session and publishes sessions atomically |
| 4 | Same file stored twice (rare) | Application crashed after `upload()` returned, before recording the result | `upload(..., idempotency_key=...)` maps an application key to the same upload |
| 5 | Half-deleted session directories | Server crash, or a late part request, while a finished session was being deleted | Sessions are renamed to `.trash-*` before deletion; `cleanup_expired()` collects all crash debris |

| Code | S4 runs | Passed | Failed files | Duplicates | Leftover locks | Unfinished sessions |
|---|---|---|---|---|---|---|
| original | 8 | 0 | 21 | 8 | 21 | 29 |
| fixes 1–2 | 8 | 4 | 0 | 0 | 0 | 5 |
| final | 10 + 1 full-size | 11 | 0 | 0 | 0 | 0 |

## Reproduce

```bash
pip install -e ".[bench]"
python benchmarks/load/run_load_test.py --images 800 --sessions 10 --server-workers 4
```
