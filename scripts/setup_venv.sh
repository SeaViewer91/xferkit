#!/usr/bin/env bash
# Create .venv in the project root and install xferkit in editable mode with dev tools.
# Usage: ./scripts/setup_venv.sh [python-executable]   (default: python3)
set -euo pipefail

cd "$(dirname "$0")/.."
PYTHON="${1:-python3}"

"$PYTHON" -c 'import sys; assert sys.version_info >= (3, 10), "xferkit needs Python 3.10+"'

if [ ! -d .venv ]; then
  "$PYTHON" -m venv .venv
fi

.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -e ".[dev]"

echo
echo "Done. Activate with:  source .venv/bin/activate"
echo "Run the tests with:   pytest"
