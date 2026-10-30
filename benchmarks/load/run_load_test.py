"""xferkit load test: thousands of ~10 MB images, ~10 concurrent client sessions,
a multi-worker server, and fault injection.

    pip install -e ".[bench]"
    python benchmarks/load/run_load_test.py --images 800 --sessions 10 --server-workers 4

Scenarios (pick with --scenarios, default all):
  S0  smoke             1 session, 50 images, upload + download
  S1  baseline          1 session, all images, upload + download
  S2  concurrent        N sessions upload all images at once, then download at once
  S3  mixed             half the sessions upload new images while the other half
                        download earlier ones
  S4  fault injection   N sessions upload; 3 sessions are SIGKILLed at 25 %, the whole
                        server is SIGKILLed and restarted at 50 %, the killed sessions
                        are restarted with --resume; then everything is downloaded
  S5  concurrency sweep files in flight per session = 1 / 4 / 8 (200-image subset)

Every scenario starts a fresh server and store, checks every stored file against
the manifest's SHA-256 on the server side, verifies every download on the client
side, and inspects the store for leftovers (pending sessions, locks, temp files).
Results go to <data-dir>/results/<timestamp>/report.md and report.json.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
import psutil

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from gen_images import generate  # noqa: E402

import xferkit  # noqa: E402
from xferkit import LocalStorage  # noqa: E402

GiB = 2**30
MiB = 2**20


# ---------------------------------------------------------------------------- server


class ServerProcess:
    """uvicorn with N workers in its own process group."""

    def __init__(self, store: Path, port: int, workers: int, log_path: Path) -> None:
        self.store = store
        self.port = port
        self.workers = workers
        self.log_path = log_path
        self.proc: subprocess.Popen | None = None

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self) -> None:
        env = {**os.environ, "XFERKIT_LOAD_STORE": str(self.store), "PYTHONUNBUFFERED": "1"}
        log = open(self.log_path, "ab")  # noqa: SIM115
        self.proc = subprocess.Popen(
            [
                sys.executable, "-m", "uvicorn", "server:app",
                "--app-dir", str(HERE),
                "--host", "127.0.0.1", "--port", str(self.port),
                "--workers", str(self.workers),
                "--log-level", "warning", "--no-access-log",
            ],
            env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
        )  # fmt: skip
        deadline = time.monotonic() + 30
        while True:
            try:
                if httpx.get(f"{self.url}/healthz", timeout=1).status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            if self.proc.poll() is not None:
                raise RuntimeError(f"server exited, see {self.log_path}")
            if time.monotonic() > deadline:
                raise RuntimeError("server did not become ready")
            time.sleep(0.2)
        time.sleep(1.0)  # let the remaining workers finish booting

    def kill(self) -> None:
        if self.proc and self.proc.poll() is None:
            os.killpg(self.proc.pid, signal.SIGKILL)
            self.proc.wait()

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            os.killpg(self.proc.pid, signal.SIGTERM)
            try:
                self.proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.kill()


# ---------------------------------------------------------------------------- sampling


class Sampler(threading.Thread):
    """Samples CPU / RSS / open files of the server tree and the client sessions."""

    def __init__(self, interval: float = 1.0) -> None:
        super().__init__(daemon=True)
        self.interval = interval
        self.server_pid: int | None = None
        self.client_pids: set[int] = set()
        self.samples: list[dict] = []
        self._cache: dict[int, psutil.Process] = {}
        self._stop_event = threading.Event()

    def _proc(self, pid: int) -> psutil.Process:
        proc = self._cache.get(pid)
        if proc is None:
            proc = self._cache[pid] = psutil.Process(pid)
            proc.cpu_percent(None)
        return proc

    def _measure(self, pids: list[int]) -> dict[str, float]:
        totals = {"rss_mib": 0.0, "cpu_pct": 0.0, "fds": 0, "threads": 0, "procs": 0}
        for pid in pids:
            try:
                proc = self._proc(pid)
                with proc.oneshot():
                    totals["rss_mib"] += proc.memory_info().rss / MiB
                    totals["cpu_pct"] += proc.cpu_percent(None)
                    totals["fds"] += proc.num_fds() if hasattr(proc, "num_fds") else 0
                    totals["threads"] += proc.num_threads()
                    totals["procs"] += 1
            except psutil.Error:
                continue
        return totals

    def run(self) -> None:
        while not self._stop_event.wait(self.interval):
            server: list[int] = []
            if self.server_pid:
                try:
                    root = psutil.Process(self.server_pid)
                    server = [root.pid] + [c.pid for c in root.children(recursive=True)]
                except psutil.Error:
                    server = []
            self.samples.append(
                {
                    "t": time.time(),
                    "server": self._measure(server),
                    "clients": self._measure(list(self.client_pids)),
                }
            )

    def stop(self) -> None:
        self._stop_event.set()
        self.join(timeout=5)

    def summary(self, since: float = 0.0) -> dict[str, Any]:
        rows = [s for s in self.samples if s["t"] >= since]
        out: dict[str, Any] = {}
        for kind in ("server", "clients"):
            values = [r[kind] for r in rows if r[kind]["procs"]]
            if not values:
                continue
            out[kind] = {
                "peak_rss_mib": round(max(v["rss_mib"] for v in values), 1),
                "mean_rss_mib": round(statistics.fmean(v["rss_mib"] for v in values), 1),
                "peak_cpu_pct": round(max(v["cpu_pct"] for v in values), 1),
                "mean_cpu_pct": round(statistics.fmean(v["cpu_pct"] for v in values), 1),
                "peak_fds": max(v["fds"] for v in values),
                "peak_threads": max(v["threads"] for v in values),
            }
            if kind == "server":
                first = values[: max(1, len(values) // 5)]
                last = values[-max(1, len(values) // 5) :]
                out[kind]["rss_first_fifth_mib"] = round(
                    statistics.fmean(v["rss_mib"] for v in first), 1
                )
                out[kind]["rss_last_fifth_mib"] = round(
                    statistics.fmean(v["rss_mib"] for v in last), 1
                )
        return out


# ---------------------------------------------------------------------------- sessions


@dataclass
class SessionSpec:
    role: str
    items: list[dict]
    name: str
    resume: bool = False


@dataclass
class Context:
    args: argparse.Namespace
    images_dir: Path
    manifest: dict[str, dict]
    work: Path
    server: ServerProcess
    sampler: Sampler
    file_concurrency: int


def launch(ctx: Context, spec: SessionSpec) -> subprocess.Popen:
    session_dir = ctx.work / "sessions" / spec.name
    session_dir.mkdir(parents=True, exist_ok=True)
    items_file = session_dir / "items.json"
    items_file.write_text(json.dumps(spec.items, ensure_ascii=False), encoding="utf-8")
    cmd = [
        sys.executable, str(HERE / "session_worker.py"),
        "--role", spec.role,
        "--server", ctx.server.url,
        "--items", str(items_file),
        "--out", str(session_dir / "results.jsonl"),
        "--summary", str(session_dir / f"summary-{int(time.time() * 1000)}.json"),
        "--file-concurrency", str(ctx.file_concurrency),
        "--part-concurrency", str(ctx.args.part_concurrency),
        "--segment-size", str(ctx.args.segment_size),
    ]  # fmt: skip
    if spec.role == "upload":
        cmd += [
            "--images-dir", str(ctx.images_dir),
            "--state-dir", str(session_dir / "state"),
            "--key-prefix", f"{ctx.work.name}/{spec.name}",
        ]  # fmt: skip
    else:
        cmd += ["--download-dir", str(ctx.work / "downloads" / spec.name)]
    if spec.resume:
        cmd.append("--resume")
    log = open(session_dir / "session.log", "ab")  # noqa: SIM115
    proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
    ctx.sampler.client_pids.add(proc.pid)
    return proc


def session_records(ctx: Context, spec: SessionSpec) -> list[dict]:
    """Final record per file (a later success overrides an earlier failure)."""
    path = ctx.work / "sessions" / spec.name / "results.jsonl"
    latest: dict[str, dict] = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            if rec.get("ok") or not latest.get(rec["name"], {}).get("ok"):
                latest[rec["name"]] = rec
    return list(latest.values())


def session_summaries(ctx: Context, spec: SessionSpec) -> list[dict]:
    session_dir = ctx.work / "sessions" / spec.name
    return [json.loads(p.read_text()) for p in sorted(session_dir.glob("summary-*.json"))]


def ok_count(ctx: Context, spec: SessionSpec) -> int:
    path = ctx.work / "sessions" / spec.name / "results.jsonl"
    if not path.exists():
        return 0
    return path.read_text(encoding="utf-8").count('"ok": true')


def split(items: list[dict], parts: int) -> list[list[dict]]:
    return [items[i::parts] for i in range(parts)]


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    k = (len(ordered) - 1) * pct / 100
    lo, hi = int(k), min(int(k) + 1, len(ordered) - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (k - lo)


def phase_metrics(ctx: Context, label: str, specs: list[SessionSpec], wall: float) -> dict:
    records = [r for spec in specs for r in session_records(ctx, spec)]
    summaries = [s for spec in specs for s in session_summaries(ctx, spec)]
    expected = sum(len(spec.items) for spec in specs)
    ok = [r for r in records if r.get("ok")]
    seconds = [r["seconds"] for r in ok]
    nbytes = sum(r["bytes"] for r in ok)
    statuses: Counter[str] = Counter()
    for summary in summaries:
        statuses.update(summary.get("status_counts", {}))
    errors = Counter(r["error"].split(":")[0] for r in records if not r.get("ok"))
    return {
        "label": label,
        "sessions": len(specs),
        "files_expected": expected,
        "files_ok": len(ok),
        "files_failed": expected - len(ok),
        "gib": round(nbytes / GiB, 2),
        "wall_s": round(wall, 1),
        "mib_per_s": round(nbytes / MiB / wall, 1) if wall else 0.0,
        "files_per_s": round(len(ok) / wall, 2) if wall else 0.0,
        "file_p50_s": round(percentile(seconds, 50), 2),
        "file_p95_s": round(percentile(seconds, 95), 2),
        "file_p99_s": round(percentile(seconds, 99), 2),
        "file_max_s": round(max(seconds, default=0.0), 2),
        "retries": sum(s.get("retries", 0) for s in summaries),
        "http_errors": {k: v for k, v in statuses.items() if int(k) >= 400},
        "resumed_files": sum(1 for r in ok if r.get("resumed")),
        "verified_downloads": sum(1 for r in ok if r.get("verified")),
        "error_types": dict(errors),
        "error_samples": list({r["error"] for r in records if not r.get("ok")})[:5],
    }


def run_phase(ctx: Context, label: str, specs: list[SessionSpec]) -> dict:
    print(
        f"  - {label}: {len(specs)} session(s), {sum(len(s.items) for s in specs)} files",
        flush=True,
    )
    started = time.time()
    procs = [launch(ctx, spec) for spec in specs]
    for proc in procs:
        proc.wait()
    metrics = phase_metrics(ctx, label, specs, time.time() - started)
    print(
        f"    {metrics['files_ok']}/{metrics['files_expected']} ok, {metrics['gib']} GiB in "
        f"{metrics['wall_s']}s = {metrics['mib_per_s']} MiB/s, failed={metrics['files_failed']}, "
        f"retries={metrics['retries']}",
        flush=True,
    )
    return metrics


def uploaded_items(ctx: Context, specs: list[SessionSpec]) -> list[dict]:
    items = []
    for spec in specs:
        for rec in session_records(ctx, spec):
            if rec.get("ok"):
                items.append({**ctx.manifest[rec["name"]], "upload_id": rec["upload_id"]})
    return items


# ---------------------------------------------------------------------------- store checks


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        while block := fh.read(8 * MiB):
            digest.update(block)
    return digest.hexdigest()


def inspect_store(ctx: Context, uploads: list[dict]) -> dict:
    store = ctx.server.store
    session_dirs = [p for p in (store / "uploads").iterdir() if p.is_dir()]
    pending = sorted(
        p.name for p in session_dirs if not p.name.startswith(".") and (p / "session.json").exists()
    )
    # Leftovers of server crashes (half-created / half-removed sessions, temp
    # files). Clients never see them; garbage collection must remove all of them.
    crash_leftovers = [p.name for p in session_dirs if p.name not in pending]
    crash_leftovers += [str(p.relative_to(store)) for p in store.rglob(".*.tmp")]
    collected = LocalStorage(store).collect_garbage(older_than=0)
    remaining_after_gc = [
        str(p.relative_to(store)) for p in (store / "uploads").iterdir() if p.name not in pending
    ] + [str(p.relative_to(store)) for p in store.rglob(".*.tmp")]
    tokens = store / "tokens"
    bound = (
        {t.read_text().strip() for t in tokens.iterdir() if t.is_file()}
        if tokens.is_dir()
        else set()
    )
    pending_details = [
        {
            "upload_id": name,
            "has_session_json": (store / "uploads" / name / "session.json").exists(),
            "parts": len(list((store / "uploads" / name / "parts").glob("*.json"))),
            "token_bound": name in bound,
        }
        for name in pending[:5]
    ]
    locks = [str(p.relative_to(store)) for p in store.rglob("*.lock")]
    temps = remaining_after_gc
    completing = [str(p.relative_to(store)) for p in store.rglob(".completing")]
    record_ids = {p.stem for p in (store / "records").glob("*.json")}
    acknowledged = {item["upload_id"] for item in uploads}

    def check(item: dict) -> str | None:
        record_path = store / "records" / f"{item['upload_id']}.json"
        if not record_path.exists():
            return f"{item['name']}: no completion record"
        record = json.loads(record_path.read_text(encoding="utf-8"))
        path = store / record["path"]
        if record["size"] != item["size"] or path.stat().st_size != item["size"]:
            return f"{item['name']}: size mismatch"
        if sha256_file(path) != item["sha256"]:
            return f"{item['name']}: SHA-256 mismatch"
        return None

    with ThreadPoolExecutor(max(2, os.cpu_count() or 2)) as pool:
        problems = [p for p in pool.map(check, uploads) if p]
    return {
        "server_verified_files": len(uploads) - len(problems),
        "server_verify_problems": problems[:10],
        "server_verify_problem_count": len(problems),
        "completion_records": len(record_ids),
        "records_not_acknowledged_by_client": len(record_ids - acknowledged),
        "unacknowledged_details": [
            json.loads((store / "records" / f"{rid}.json").read_text(encoding="utf-8"))["filename"]
            for rid in sorted(record_ids - acknowledged)[:5]
        ],
        "pending_sessions": len(pending),
        "pending_details": pending_details,
        "crash_leftovers_found": len(crash_leftovers),
        "crash_leftovers_collected": collected,
        "leftover_locks": locks[:10],
        "leftover_lock_count": len(locks),
        "leftover_tmp_count": len(temps),
        "leftover_completing_count": len(completing),
    }


# ---------------------------------------------------------------------------- scenarios


def fresh_server(ctx: Context, name: str) -> None:
    store = ctx.work / "stores" / name
    shutil.rmtree(store, ignore_errors=True)
    store.mkdir(parents=True)
    ctx.server = ServerProcess(
        store, ctx.args.port, ctx.args.server_workers, ctx.work / f"server-{name}.log"
    )
    ctx.server.start()
    ctx.sampler.server_pid = ctx.server.proc.pid  # type: ignore[union-attr]


def finish_server(ctx: Context, keep: bool) -> None:
    ctx.server.stop()
    if not keep:
        shutil.rmtree(ctx.server.store, ignore_errors=True)
        shutil.rmtree(ctx.work / "downloads", ignore_errors=True)


def round_trip(ctx: Context, name: str, images: list[dict], sessions: int) -> dict:
    up_specs = [
        SessionSpec("upload", part, f"{name}-up-{i}")
        for i, part in enumerate(split(images, sessions))
    ]
    upload = run_phase(ctx, "upload", up_specs)
    uploads = uploaded_items(ctx, up_specs)
    down_specs = [
        SessionSpec("download", part, f"{name}-down-{i}")
        for i, part in enumerate(split(uploads, sessions))
    ]
    download = run_phase(ctx, "download", down_specs)
    return {"phases": [upload, download], "store": inspect_store(ctx, uploads)}


def scenario_s3(ctx: Context, images: list[dict], sessions: int) -> dict:
    half = len(images) // 2
    seed_specs = [SessionSpec("upload", images[:half], "S3-seed")]
    seed = run_phase(ctx, "seed upload (1 session, not part of the mixed load)", seed_specs)
    seeded = uploaded_items(ctx, seed_specs)
    n_up = max(1, sessions // 2)
    n_down = max(1, sessions - n_up)
    up_specs = [
        SessionSpec("upload", p, f"S3-up-{i}") for i, p in enumerate(split(images[half:], n_up))
    ]
    down_specs = [
        SessionSpec("download", p, f"S3-down-{i}") for i, p in enumerate(split(seeded, n_down))
    ]
    print(f"  - mixed: {n_up} upload + {n_down} download sessions at once", flush=True)
    started = time.time()
    procs = [launch(ctx, s) for s in up_specs + down_specs]
    for proc in procs:
        proc.wait()
    wall = time.time() - started
    mixed_up = phase_metrics(ctx, f"mixed: upload ({n_up} sessions)", up_specs, wall)
    mixed_down = phase_metrics(ctx, f"mixed: download ({n_down} sessions)", down_specs, wall)
    total_bytes = (mixed_up["gib"] + mixed_down["gib"]) * GiB
    for m in (mixed_up, mixed_down):
        print(
            f"    {m['label']}: {m['files_ok']}/{m['files_expected']} ok, failed={m['files_failed']}"
        )
    print(f"    combined {total_bytes / MiB / wall:.1f} MiB/s", flush=True)
    uploads = seeded + uploaded_items(ctx, up_specs)
    return {
        "phases": [seed, mixed_up, mixed_down],
        "combined_mib_per_s": round(total_bytes / MiB / wall, 1),
        "store": inspect_store(ctx, uploads),
    }


def scenario_s4(ctx: Context, images: list[dict], sessions: int) -> dict:
    specs = [SessionSpec("upload", p, f"S4-up-{i}") for i, p in enumerate(split(images, sessions))]
    victims = specs[: min(3, len(specs))]
    total = len(images)
    events: list[str] = []
    print(f"  - upload with faults: {len(specs)} sessions, {total} files", flush=True)
    started = time.time()
    procs = {spec.name: launch(ctx, spec) for spec in specs}
    killed = restarted = False

    def note(text: str) -> None:
        done = sum(ok_count(ctx, s) for s in specs)
        line = f"t={time.time() - started:5.1f}s done={done}/{total}: {text}"
        events.append(line)
        print(f"    {line}", flush=True)

    while True:
        done = sum(ok_count(ctx, s) for s in specs)
        if not killed and done >= 0.25 * total:
            for spec in victims:
                procs[spec.name].send_signal(signal.SIGKILL)
                procs[spec.name].wait()
            killed = True
            note(f"SIGKILL client sessions {[s.name for s in victims]}")
        if killed and not restarted and done >= 0.5 * total:
            ctx.server.kill()
            note("SIGKILL whole server (master + all workers)")
            time.sleep(3)
            ctx.server.start()
            ctx.sampler.server_pid = ctx.server.proc.pid  # type: ignore[union-attr]
            note("server restarted on the same store")
            for spec in victims:
                spec.resume = True
                procs[spec.name] = launch(ctx, spec)
            restarted = True
            note("killed sessions restarted with --resume")
        if all(p.poll() is not None for p in procs.values()):
            if killed and not restarted:  # the run finished before the 50 % mark
                ctx.server.kill()
                ctx.server.start()
                for spec in victims:
                    spec.resume = True
                    procs[spec.name] = launch(ctx, spec)
                restarted = True
                continue
            break
        time.sleep(0.25)
    upload = phase_metrics(ctx, "upload with faults", specs, time.time() - started)
    print(
        f"    {upload['files_ok']}/{upload['files_expected']} ok, failed={upload['files_failed']}, "
        f"resumed={upload['resumed_files']}, retries={upload['retries']}",
        flush=True,
    )
    uploads = uploaded_items(ctx, specs)
    down_specs = [
        SessionSpec("download", p, f"S4-down-{i}") for i, p in enumerate(split(uploads, sessions))
    ]
    download = run_phase(ctx, "download after faults", down_specs)
    return {"phases": [upload, download], "events": events, "store": inspect_store(ctx, uploads)}


def scenario_s5(ctx: Context, images: list[dict], sessions: int) -> dict:
    subset = images[: min(200, len(images))]
    rows = []
    for fc in (1, 4, 8):
        fresh_server(ctx, f"S5-fc{fc}")
        ctx.file_concurrency = fc
        specs = [
            SessionSpec("upload", p, f"S5-fc{fc}-up-{i}")
            for i, p in enumerate(split(subset, sessions))
        ]
        metrics = run_phase(ctx, f"upload, {fc} file(s) in flight per session", specs)
        metrics["store"] = inspect_store(ctx, uploaded_items(ctx, specs))
        rows.append(metrics)
        finish_server(ctx, ctx.args.keep)
    ctx.file_concurrency = ctx.args.file_concurrency
    return {"phases": rows}


def evaluate(result: dict) -> list[str]:
    """Return the list of failed acceptance checks (empty = pass)."""
    failures = []
    for phase in result.get("phases", []):
        if phase["files_failed"]:
            failures.append(f"{phase['label']}: {phase['files_failed']} file(s) failed")
        if "download" in phase["label"] and phase["verified_downloads"] != phase["files_ok"]:
            failures.append(f"{phase['label']}: not every download was verified")
        if "store" in phase:
            failures += [f"{phase['label']}: {f}" for f in _store_failures(phase["store"])]
    if "store" in result:
        failures += _store_failures(result["store"])
    return failures


def _store_failures(store: dict) -> list[str]:
    failures = []
    if store["server_verify_problem_count"]:
        failures.append(
            f"{store['server_verify_problem_count']} stored file(s) failed verification"
        )
    if (
        store["leftover_lock_count"]
        or store["leftover_tmp_count"]
        or store["leftover_completing_count"]
    ):
        failures.append(
            f"leftovers: {store['leftover_lock_count']} lock, "
            f"{store['leftover_tmp_count']} not removable by garbage collection, "
            f"{store['leftover_completing_count']} completing"
        )
    if store["pending_sessions"]:
        failures.append(f"{store['pending_sessions']} unfinished session(s) left in the store")
    if store["records_not_acknowledged_by_client"]:
        failures.append(
            f"{store['records_not_acknowledged_by_client']} completed upload(s) the client never "
            "acknowledged (duplicate copies of a file)"
        )
    return failures


# ---------------------------------------------------------------------------- report


def render_report(meta: dict, results: dict[str, dict]) -> str:
    lines = [
        "# xferkit load test report",
        "",
        f"- Date: {meta['date']}",
        f"- xferkit {meta['xferkit']}, Python {meta['python']}, {meta['platform']}",
        f"- Machine: {meta['cpus']} CPU(s), {meta['ram_gib']} GiB RAM",
        f"- Images: {meta['images']} × ~{meta['avg_mib']} MiB = {meta['total_gib']} GiB (5472×3648 JPEG)",
        f"- Server: uvicorn, {meta['server_workers']} worker processes, one shared LocalStorage",
        f"- Clients: {meta['sessions']} sessions (separate processes), "
        f"{meta['file_concurrency']} files × {meta['part_concurrency']} parts in flight per session",
        "",
        "## Summary",
        "",
        "| Scenario | Result | Phase | Files ok / expected | GiB | Wall s | MiB/s | p50 / p95 / p99 s per file | Retries |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for name, result in results.items():
        verdict = "PASS" if not result["failures"] else "FAIL"
        for i, phase in enumerate(result["phases"]):
            lines.append(
                f"| {name if i == 0 else ''} | {verdict if i == 0 else ''} | {phase['label']} | "
                f"{phase['files_ok']} / {phase['files_expected']} | {phase['gib']} | {phase['wall_s']} | "
                f"{phase['mib_per_s']} | {phase['file_p50_s']} / {phase['file_p95_s']} / {phase['file_p99_s']} | "
                f"{phase['retries']} |"
            )
    for name, result in results.items():
        lines += ["", f"## {name} — {result['title']}", ""]
        if result["failures"]:
            lines += ["**Failed checks:**", ""] + [f"- {f}" for f in result["failures"]] + [""]
        else:
            lines += ["All checks passed.", ""]
        if result.get("events"):
            lines += ["Fault-injection timeline:", "", "```", *result["events"], "```", ""]
        if "combined_mib_per_s" in result:
            lines += [f"Combined mixed throughput: {result['combined_mib_per_s']} MiB/s", ""]
        lines += [
            "```json",
            json.dumps(
                {k: v for k, v in result.items() if k not in ("title",)},
                ensure_ascii=False,
                indent=1,
            ),
            "```",
        ]
    return "\n".join(lines) + "\n"


TITLES = {
    "S0": "smoke test",
    "S1": "single-session baseline",
    "S2": "concurrent sessions",
    "S3": "mixed upload + download",
    "S4": "fault injection (client kills, server crash/restart, resume)",
    "S5": "concurrency sweep",
}


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="xferkit load test",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--images", type=int, default=800)
    parser.add_argument("--data-dir", type=Path, default=Path("loadtest-data"))
    parser.add_argument("--sessions", type=int, default=10)
    parser.add_argument("--server-workers", type=int, default=4)
    parser.add_argument(
        "--file-concurrency", type=int, default=4, help="files in flight per session"
    )
    parser.add_argument(
        "--part-concurrency", type=int, default=2, help="parts/segments in flight per file"
    )
    parser.add_argument("--segment-size", type=int, default=4 * MiB, help="download range size")
    parser.add_argument("--scenarios", default="S0,S1,S2,S3,S4,S5")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument(
        "--keep", action="store_true", help="keep server stores after each scenario"
    )
    args = parser.parse_args()
    args.port = args.port or free_port()

    data_dir = args.data_dir.resolve()
    images_dir = data_dir / "images"
    print(f"[1/3] preparing {args.images} images in {images_dir}", flush=True)
    manifest = generate(args.images, images_dir)
    images = manifest["images"]
    total_bytes = manifest["total_bytes"]

    need = total_bytes * 1.3 + 2 * GiB  # one store copy + slack (downloads are deleted as verified)
    free = shutil.disk_usage(data_dir).free
    if free < need:
        sys.exit(f"not enough disk: need ~{need / GiB:.1f} GiB free, have {free / GiB:.1f} GiB")

    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    work = data_dir / "results" / stamp
    work.mkdir(parents=True)
    sampler = Sampler()
    sampler.start()
    ctx = Context(
        args, images_dir, {i["name"]: i for i in images}, work, None, sampler, args.file_concurrency
    )  # type: ignore[arg-type]

    print(f"[2/3] running scenarios {args.scenarios} -> {work}", flush=True)
    results: dict[str, dict] = {}
    for name in [s.strip().upper() for s in args.scenarios.split(",") if s.strip()]:
        print(f"\n== {name}: {TITLES[name]}", flush=True)
        started = time.time()
        if name == "S5":
            result = scenario_s5(ctx, images, args.sessions)
        else:
            fresh_server(ctx, name)
            try:
                if name == "S0":
                    result = round_trip(ctx, name, images[:50], 1)
                elif name == "S1":
                    result = round_trip(ctx, name, images, 1)
                elif name == "S2":
                    result = round_trip(ctx, name, images, args.sessions)
                elif name == "S3":
                    result = scenario_s3(ctx, images, args.sessions)
                elif name == "S4":
                    result = scenario_s4(ctx, images, args.sessions)
                else:
                    raise SystemExit(f"unknown scenario {name}")
            finally:
                finish_server(ctx, args.keep)
        result["title"] = TITLES[name]
        result["resources"] = sampler.summary(since=started)
        result["failures"] = evaluate(result)
        results[name] = result
        verdict = "PASS" if not result["failures"] else "FAIL: " + "; ".join(result["failures"])
        print(f"   => {name} {verdict}", flush=True)
        (work / "report.json").write_text(
            json.dumps(results, ensure_ascii=False, indent=1), encoding="utf-8"
        )

    sampler.stop()
    (work / "samples.json").write_text(json.dumps(sampler.samples), encoding="utf-8")
    sizes = [i["size"] for i in images]
    meta = {
        "date": datetime.now().isoformat(timespec="seconds"),
        "xferkit": xferkit.__version__,
        "python": platform.python_version(),
        "platform": f"{platform.system()} {platform.machine()}",
        "cpus": os.cpu_count(),
        "ram_gib": round(psutil.virtual_memory().total / GiB, 1),
        "images": len(images),
        "avg_mib": round(statistics.fmean(sizes) / MiB, 1),
        "total_gib": round(total_bytes / GiB, 2),
        "server_workers": args.server_workers,
        "sessions": args.sessions,
        "file_concurrency": args.file_concurrency,
        "part_concurrency": args.part_concurrency,
    }
    report = render_report(meta, results)
    (work / "report.md").write_text(report, encoding="utf-8")
    (work / "report.json").write_text(
        json.dumps({"meta": meta, "results": results}, ensure_ascii=False, indent=1),
        encoding="utf-8",
    )
    print(f"\n[3/3] report: {work / 'report.md'}")
    failed = [name for name, r in results.items() if r["failures"]]
    print("ALL SCENARIOS PASSED" if not failed else f"FAILED: {', '.join(failed)}")
    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
