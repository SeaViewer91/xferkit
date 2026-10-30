"""Generate synthetic drone-like JPEG images (~10 MB each) and a manifest.

Images are 5472x3648 (20 MP, the sensor size of common survey drones), made of a
colour gradient, sensor-like noise and random rectangular "objects". The noise
keeps JPEG from compressing them much, so each file is about 9-11 MB. Generation
is deterministic for a given seed.

    python benchmarks/load/gen_images.py --count 800 --out loadtest-data/images
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
from PIL import Image

HEIGHT, WIDTH = 3648, 5472
_PAD = 256
_NOISE: np.ndarray | None = None


def image_name(index: int) -> str:
    # Mostly camera-style names, every 10th a Korean name to exercise Unicode paths.
    if index % 10 == 9:
        return f"현장사진_{index:05d}.jpg"
    return f"DJI_{index:05d}.JPG"


def _init_worker(seed: int) -> None:
    global _NOISE
    rng = np.random.default_rng(seed)
    _NOISE = rng.standard_normal((HEIGHT + _PAD, WIDTH + _PAD, 3), dtype=np.float32)


def _make_image(job: tuple[int, str, int]) -> dict:
    index, out_dir, seed = job
    assert _NOISE is not None
    rng = np.random.default_rng(seed * 1_000_003 + index)
    dy, dx = (int(v) for v in rng.integers(0, _PAD, 2))
    y = np.linspace(0, 1, HEIGHT, dtype=np.float32)[:, None, None]
    x = np.linspace(0, 1, WIDTH, dtype=np.float32)[None, :, None]
    top_colour, side_colour = rng.uniform(30, 200, (2, 3)).astype(np.float32)
    img = top_colour * (1 - y) + side_colour * x * 0.5
    img = img + float(rng.uniform(15, 21)) * _NOISE[dy : dy + HEIGHT, dx : dx + WIDTH]
    for _ in range(12):
        h, w = (int(v) for v in rng.integers(40, 400, 2))
        top = int(rng.integers(0, HEIGHT - h))
        left = int(rng.integers(0, WIDTH - w))
        img[top : top + h, left : left + w] += rng.uniform(-60, 60, 3).astype(np.float32)
    pixels = np.clip(img, 0, 255).astype(np.uint8)

    path = Path(out_dir) / image_name(index)
    Image.fromarray(pixels).save(path, "JPEG", quality=92)
    data = path.read_bytes()
    return {
        "index": index,
        "name": path.name,
        "size": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
    }


def generate(
    count: int, out_dir: Path, *, seed: int = 20261031, workers: int | None = None
) -> dict:
    """Create ``count`` images in ``out_dir`` (reusing a matching earlier run)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if (
            manifest.get("seed") == seed
            and manifest.get("count") == count
            and all(
                (out_dir / item["name"]).is_file()
                and (out_dir / item["name"]).stat().st_size == item["size"]
                for item in manifest["images"]
            )
        ):
            print(f"reusing {count} existing images in {out_dir}")
            return manifest

    workers = workers or max(1, min(os.cpu_count() or 1, 4))
    started = time.monotonic()
    images: list[dict] = []
    jobs = [(i, str(out_dir), seed) for i in range(count)]
    with ProcessPoolExecutor(workers, initializer=_init_worker, initargs=(seed,)) as pool:
        for item in pool.map(_make_image, jobs, chunksize=4):
            images.append(item)
            if len(images) % 50 == 0 or len(images) == count:
                elapsed = time.monotonic() - started
                print(f"  {len(images)}/{count} images ({elapsed:.0f}s)", flush=True)
    manifest = {
        "seed": seed,
        "count": count,
        "total_bytes": sum(item["size"] for item in images),
        "images": images,
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--count", type=int, default=800)
    parser.add_argument("--out", type=Path, default=Path("loadtest-data/images"))
    parser.add_argument("--seed", type=int, default=20261031)
    parser.add_argument("--workers", type=int)
    args = parser.parse_args()
    manifest = generate(args.count, args.out, seed=args.seed, workers=args.workers)
    sizes = [item["size"] for item in manifest["images"]]
    print(
        f"{len(sizes)} images, {sum(sizes) / 2**30:.2f} GiB, "
        f"{min(sizes) / 2**20:.1f}-{max(sizes) / 2**20:.1f} MiB each"
    )


if __name__ == "__main__":
    main()
