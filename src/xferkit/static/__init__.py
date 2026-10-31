"""Static assets shipped with the Python package."""

from pathlib import Path

#: The browser client (ES module), built from ``clients/js``.
BROWSER_CLIENT = Path(__file__).with_name("xferkit.mjs")
