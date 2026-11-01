"""Fail unless a release tag matches every version number in the repository.

    python scripts/check_release_version.py v0.1.0

Checks src/xferkit/_version.py, clients/js/package.json, the VERSION constant
of the browser client and the CHANGELOG heading, so a release never ships
mismatched Python and npm packages.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def found_versions() -> dict[str, str | None]:
    def search(path: str, pattern: str) -> str | None:
        match = re.search(pattern, (ROOT / path).read_text(encoding="utf-8"), re.MULTILINE)
        return match.group(1) if match else None

    package = json.loads((ROOT / "clients/js/package.json").read_text(encoding="utf-8"))
    return {
        "src/xferkit/_version.py": search("src/xferkit/_version.py", r'__version__ = "([^"]+)"'),
        "clients/js/package.json": package.get("version"),
        "clients/js/src/index.ts (VERSION)": search(
            "clients/js/src/index.ts", r'export const VERSION = "([^"]+)"'
        ),
    }


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: check_release_version.py <tag, e.g. v0.1.0>", file=sys.stderr)
        return 2
    tag = sys.argv[1]
    if not re.fullmatch(r"v\d+\.\d+\.\d+([a-z]+\d+)?", tag):
        print(f"tag {tag!r} is not of the form v1.2.3 (or v1.2.3rc1)", file=sys.stderr)
        return 1
    version = tag[1:]
    problems = [
        f"  {where}: {value}" for where, value in found_versions().items() if value != version
    ]
    changelog = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    if not re.search(rf"^## {re.escape(version)}\b", changelog, re.MULTILINE):
        problems.append(f"  CHANGELOG.md: no '## {version}' heading")
    if problems:
        print(f"tag {tag} does not match:", *problems, sep="\n", file=sys.stderr)
        return 1
    print(f"all versions are {version}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
