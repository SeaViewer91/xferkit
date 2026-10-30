# Create .venv in the project root and install xferkit in editable mode with dev tools.
# Usage: powershell -ExecutionPolicy Bypass -File scripts\setup_venv.ps1 [python-executable]
param([string]$Python = "py")

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

& $Python -c "import sys; assert sys.version_info >= (3, 10), 'xferkit needs Python 3.10+'"

if (-not (Test-Path ".venv")) {
    & $Python -m venv .venv
}

& .\.venv\Scripts\python.exe -m pip install --upgrade pip
& .\.venv\Scripts\python.exe -m pip install -e ".[dev]"

Write-Host ""
Write-Host "Done. Activate with:  .\.venv\Scripts\Activate.ps1"
Write-Host "Run the tests with:   pytest"
