"""
Step 00 — download wrapper (Welsh Tithe Vectoriser only).

A thin front end over the vendored Welsh Tithe Map Downloader
(steps/00_download/tithe_downloader.py). It forwards the downloader's
subcommands unchanged and, for `export-toolkit`, fills in --toolkit-dir with
this toolkit's own root so you never have to type the path.

Run via the dispatcher (recommended):

    python run.py download discover                        # one-off: catalogue every NLW map
    python run.py download fetch --county Anglesey         # 'fetch' == the downloader's 'download'
    python run.py download export-toolkit --county Anglesey  # writes data/raw + data/parcel_points

or directly:

    python steps/00_download/download.py <subcommand> [flags...]

Environment: maptools. It already has requests, pillow, tqdm, numpy and GDAL, so
the downloader finds gdalwarp/gdaltransform from it — no separate env, no QGIS.

The downloader keeps its catalogue DB and downloaded scans in
steps/00_download/tithe_maps/ (gitignored). Anything this wrapper does not cover
can be run straight against tithe_downloader.py.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

HERE       = Path(__file__).resolve().parent
ROOT       = HERE.parents[1]                    # toolkit repo root (steps/00_download -> root)
DOWNLOADER = HERE / "tithe_downloader.py"

# Friendly aliases so `run.py download download` isn't needed.
ALIASES = {"fetch": "download", "maps": "list"}

_SUBCOMMANDS = ("discover, fetch (=download), export-toolkit, list (=maps), parcels, "
                "geopackage, georeference, coverage, metadata, quality, status, export")


def main() -> None:
    args = sys.argv[1:]
    if not args or args[0] in ("-h", "--help"):
        print(__doc__)
        print("Subcommands:", _SUBCOMMANDS)
        sys.exit(0 if args else 1)

    if not DOWNLOADER.exists():
        sys.exit(
            f"Downloader not found: {DOWNLOADER}\n"
            "Copy tithe_downloader.py into steps/00_download/ (see that folder's README)."
        )

    sub = ALIASES.get(args[0], args[0])
    rest = args[1:]

    # export-toolkit writes INTO this toolkit — supply the path automatically.
    if sub == "export-toolkit" and "--toolkit-dir" not in rest:
        rest = ["--toolkit-dir", str(ROOT), *rest]

    raise SystemExit(
        subprocess.run([sys.executable, str(DOWNLOADER), sub, *rest]).returncode
    )


if __name__ == "__main__":
    main()
