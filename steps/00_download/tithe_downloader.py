#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Welsh Tithe Map Downloader (v3)
===============================
Builds a local dataset of georeferenced tithe map scans plus apportionment
parcel point files from the National Library of Wales (places.library.wales).

For every map this produces one folder containing:

  {Parish}_{pid}.jpg               full-resolution stitched map scan
  {Parish}_{pid}.parcels.geojson   all apportionment parcels as points:
                                     - WGS84 geometry (GIS / QGIS overlay)
                                     - pixel_x / pixel_y (SAM point prompts)
                                     - all attributes (field no., land use,
                                       occupier, landowner, acreage, rent...)
  {Parish}_{pid}.vrt               GDAL VRT with embedded GCPs -- open in QGIS
  {Parish}_{pid}.jgw / .prj        affine world file fallback

Apportionment page images are NOT downloaded (the parcel points replace them).

Workflow
--------
  python tithe_downloader.py discover               # catalogue every map (one-off, ~10 min)
  python tithe_downloader.py metadata               # fetch titles / canvas IDs / sizes
  python tithe_downloader.py download --limit 5     # image + parcels + georef per map
  python tithe_downloader.py quality --pid X --set low   # exclude poor maps
  python tithe_downloader.py status                 # progress report
  python tithe_downloader.py export                 # CSV snapshot of the database

Other commands / flags
----------------------
  list --county Radnor              browse maps in the database
  list --search llan                search by parish/title substring
  download --pids "4634773,Llangynllo"   targeted download (PIDs or parish names)
  download --from-file targets.txt  targets from a text file, one per line
  download --warp                   also produce a north-up GeoTIFF (uses QGIS's GDAL)
  download --county Cardigan        restrict to one county
  download --pid 4634773            a single map
  download --image-only             skip the parcels/georeference step
  download --include-low            also download maps flagged quality='low'
  download --keep-tiles             keep the raw tile cache after stitching
  parcels --pid 4634773             (re)build just the parcel GeoJSON
  georeference --pid 4634773        (re)build GeoJSON + VRT + world file
  georeference --refetch            ignore cached GeoJSON, re-pull from API
"""

import os
import re
import csv
import glob
import json
import math
import time
import shutil
import sqlite3
import logging
import argparse
import subprocess
from io import BytesIO
from datetime import datetime
from pathlib import Path

import requests
from PIL import Image
from tqdm import tqdm

Image.MAX_IMAGE_PIXELS = None   # tithe maps are huge but trusted local files


# ──────────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────────

BASE_DIR      = Path(__file__).resolve().parent / "tithe_maps"
DB_PATH       = BASE_DIR / "tithe_maps.db"
DOWNLOADS_DIR = BASE_DIR / "downloads"
LOG_DIR       = BASE_DIR / "logs"

POINTS_API    = "https://places.library.wales/points"
MANIFEST_URL  = "https://iiif.llyfrgell.cymru/manifests/2.0/{pid}/manifest.json"
INFO_URL      = "https://iiif.llyfrgell.cymru/iiif/{canvas_id}/info.json"
VIEWER_URL    = "https://places.library.wales/viewer/{pid}"   # online map viewer
TILE_URL      = "https://iiif.llyfrgell.cymru/iiif/{canvas_id}/{x},{y},{ts},{ts}/{size}/0/default.jpg"

ROWS_PER_PAGE = 200   # server hard limit -- rows>200 returns corrupt JSON
API_DELAY     = 1.5   # seconds between API requests
TILE_DELAY    = 0.25  # seconds between tile downloads
MAX_RETRIES   = 4

MIN_GCPS      = 6     # minimum parcels needed for a polynomial fit

# The 13 historical counties of Wales as used in NLW feature IDs
WELSH_COUNTIES = [
    "Anglesey", "Brecknock", "Cardigan", "Carmarthen", "Caernarfon",
    "Denbigh", "Flint", "Glamorgan", "Merioneth", "Monmouth",
    "Montgomery", "Pembroke", "Radnor",
]
_COUNTIES_SORTED = sorted(WELSH_COUNTIES, key=len, reverse=True)


# ──────────────────────────────────────────────────────────────────────────────
# Logging / HTTP
# ──────────────────────────────────────────────────────────────────────────────

def setup_logging():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"tithe_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        handlers=[logging.FileHandler(log_file, encoding="utf-8"),
                  logging.StreamHandler()],
    )


_session = requests.Session()
_session.headers.update({
    "User-Agent": "WelshTitheMapsResearch/3.0 (academic digitisation project)"
})


def fetch_json(url, retries=MAX_RETRIES):
    for attempt in range(retries):
        try:
            resp = _session.get(url, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            logging.warning(f"Attempt {attempt+1}/{retries} failed for {url}: {exc}")
            if attempt < retries - 1:
                time.sleep(API_DELAY * (attempt + 1))
    logging.error(f"Giving up on {url}")
    return None


def fetch_bytes(url, retries=MAX_RETRIES):
    for attempt in range(retries):
        try:
            resp = _session.get(url, timeout=60)
            if resp.status_code == 200:
                return resp.content
        except Exception:
            pass
        if attempt < retries - 1:
            time.sleep(2 * (attempt + 1))
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Database
# ──────────────────────────────────────────────────────────────────────────────
# Status progression for each map:
#   discovered     -> found via /points, nothing fetched yet
#   ready          -> metadata fetched, canvas ID known, downloadable
#   downloaded     -> map image stitched and on disk
#   georeferenced  -> parcels GeoJSON + VRT + world file written
#   failed         -> something went wrong (see notes)
#
# Quality (manual flag, NULL = unassessed):
#   high / low / excluded
#   download skips 'low' and 'excluded' by default; NULL is treated as
#   downloadable so newly discovered maps flow through until you triage them.

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
    conn = get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS maps (
            map_pid         INTEGER PRIMARY KEY,
            app_pid         INTEGER,
            county          TEXT,
            parish          TEXT,
            title           TEXT,
            date            TEXT,
            scale           TEXT,
            canvas_id       INTEGER,
            width           INTEGER,
            height          INTEGER,
            parcel_count    INTEGER DEFAULT 0,
            quality         TEXT,
            status          TEXT DEFAULT 'discovered',
            image_path      TEXT,
            parcels_path    TEXT,
            georef_rms_m    REAL,
            handle_url      TEXT,
            notes           TEXT,
            discovered_date TEXT,
            downloaded_date TEXT
        );

        CREATE TABLE IF NOT EXISTS discovery_progress (
            id            INTEGER PRIMARY KEY CHECK (id = 1),
            last_page     INTEGER DEFAULT 0,
            total_fetched INTEGER DEFAULT 0,
            completed     INTEGER DEFAULT 0
        );
        INSERT OR IGNORE INTO discovery_progress (id) VALUES (1);

        CREATE INDEX IF NOT EXISTS idx_maps_county ON maps(county);
        CREATE INDEX IF NOT EXISTS idx_maps_status ON maps(status);
    """)
    # Migrations for databases created before these columns existed
    cols = {r[1] for r in conn.execute("PRAGMA table_info(maps)")}
    if "scale_factor" not in cols:
        conn.execute("ALTER TABLE maps ADD COLUMN scale_factor INTEGER DEFAULT 1")
    if "coverage_hectares" not in cols:
        # Ground area (ha) of the convex hull of a map's parcel points -- a proxy
        # for how much land the map covers. Populated by `coverage` / on fetch.
        conn.execute("ALTER TABLE maps ADD COLUMN coverage_hectares REAL")
    conn.commit()
    conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def parse_feature_id(raw_id: str):
    """Extract (county, parish) from an ID like 'CaernarfonLlanengan5c053eae8b0ce'."""
    for c in _COUNTIES_SORTED:
        if raw_id.startswith(c):
            remainder = raw_id[len(c):]
            parish_raw = re.sub(r"[0-9a-f]+$", "", remainder)  # strip hex hash
            parish = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", parish_raw).strip()
            return c, parish
    return "", raw_id


def safe_name(name: str) -> str:
    """Make a string safe for file/directory names."""
    return re.sub(r'[\\/:*?"<>|]', "_", name).strip().replace(" ", "_")


# Punctuation-insensitive matching: "Llanbedr" should find "Llan-Bedr", etc.
_NORM_STRIP = "-", " ", "'", ".", ","


def _norm(s):
    """Lowercase and drop separators so hyphen/space/apostrophe don't block a match."""
    s = (s or "").lower()
    for ch in _NORM_STRIP:
        s = s.replace(ch, "")
    return s


def _norm_sql(col):
    """SQL expression that normalises a column the same way _norm() does in Python."""
    expr = f"LOWER(COALESCE({col},''))"
    for ch in _NORM_STRIP:
        lit = "'" + ch.replace("'", "''") + "'"   # SQL-escape the quote char
        expr = f"REPLACE({expr},{lit},'')"
    return expr


def map_paths(row):
    """Return (folder, stem) for a map record's output files."""
    county = safe_name(row["county"] or "Unknown_County")
    parish = safe_name(row["parish"] or "Unknown_Parish")
    stem   = f"{parish}_{row['map_pid']}"
    return DOWNLOADS_DIR / county / stem, stem


def parcel_pixel_centre(props):
    """Centre of the map_coords pixel box, or (None, None) if absent."""
    mc = props.get("map_coords") or ""
    try:
        x, y, w, h = (int(v) for v in mc.split(","))
        return x + w // 2, y + h // 2
    except (ValueError, AttributeError):
        return None, None


# ──────────────────────────────────────────────────────────────────────────────
# DISCOVER -- page the /points API, dedupe map PIDs, count parcels per map
# ──────────────────────────────────────────────────────────────────────────────

def cmd_discover(args):
    conn = get_conn()
    prog = conn.execute("SELECT * FROM discovery_progress WHERE id=1").fetchone()

    if args.reset:
        conn.execute("UPDATE discovery_progress SET last_page=0, total_fetched=0, completed=0")
        conn.execute("UPDATE maps SET parcel_count=0")   # recounted from page 1
        conn.commit()
        prog = conn.execute("SELECT * FROM discovery_progress WHERE id=1").fetchone()
        logging.info("Discovery progress reset.")

    if prog["completed"]:
        logging.info("Discovery already complete. Use --reset to start over.")
        conn.close()
        return

    known = {r[0] for r in conn.execute("SELECT map_pid FROM maps")}
    page = prog["last_page"] + 1
    logging.info(f"Discovery from page {page} ({len(known)} maps known, "
                 f"{prog['total_fetched']} parcel records seen).")

    pages_done = 0
    while True:
        if args.max_pages and pages_done >= args.max_pages:
            logging.info(f"Stopping after --max-pages {args.max_pages}. Re-run to continue.")
            break

        url = f"{POINTS_API}?rows={ROWS_PER_PAGE}&page={page}&alt=*:*"
        data = fetch_json(url)
        if data is None:
            logging.error(f"Page {page} failed. Re-run discover to resume from here.")
            break

        features = data.get("features", [])
        if not features:
            conn.execute("UPDATE discovery_progress SET completed=1 WHERE id=1")
            conn.commit()
            logging.info("Empty page -- discovery complete.")
            break

        new_maps = {}
        counts = {}
        for feat in features:
            props = feat.get("properties", {})
            pid = props.get("map_parent_pid")
            if not pid:
                continue
            counts[pid] = counts.get(pid, 0) + 1
            if pid not in known:
                county, parish = parse_feature_id(props.get("id", ""))
                new_maps[pid] = (pid, props.get("app_parent_pid"), county, parish,
                                 datetime.now().isoformat())
                known.add(pid)

        if new_maps:
            conn.executemany(
                """INSERT OR IGNORE INTO maps
                   (map_pid, app_pid, county, parish, discovered_date)
                   VALUES (?,?,?,?,?)""",
                list(new_maps.values()),
            )
        conn.executemany(
            "UPDATE maps SET parcel_count = parcel_count + ? WHERE map_pid = ?",
            [(n, pid) for pid, n in counts.items()],
        )
        conn.execute(
            "UPDATE discovery_progress SET last_page=?, total_fetched=total_fetched+? WHERE id=1",
            (page, len(features)),
        )
        conn.commit()
        logging.info(f"Page {page}: {len(features)} parcels, +{len(new_maps)} new maps "
                     f"({len(known)} total)")

        if len(features) < ROWS_PER_PAGE:
            conn.execute("UPDATE discovery_progress SET completed=1 WHERE id=1")
            conn.commit()
            logging.info("Last page reached -- discovery complete.")
            break

        page += 1
        pages_done += 1
        time.sleep(API_DELAY)

    total = conn.execute("SELECT COUNT(*) FROM maps").fetchone()[0]
    logging.info(f"Database now holds {total} unique maps.")
    conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# METADATA -- IIIF manifest: title, date, scale, canvas ID, image dimensions
# ──────────────────────────────────────────────────────────────────────────────

def _meta_label(item):
    lbl = item.get("label", "")
    if isinstance(lbl, list):
        for l in lbl:
            if isinstance(l, dict) and l.get("@language") in ("en", ""):
                return l.get("@value", "")
        return ""
    if isinstance(lbl, dict):
        return lbl.get("@value", str(lbl))
    return str(lbl)


def _meta_first(item):
    val = item.get("value", "")
    if isinstance(val, list):
        for v in val:
            if isinstance(v, dict) and v.get("@language") in ("en", ""):
                return v.get("@value", "")
            if isinstance(v, str):
                return v
        return ""
    if isinstance(val, dict):
        return val.get("@value", str(val))
    return str(val) if val else ""


def _extract_href(s):
    m = re.search(r'href=["\']([^"\']+)["\']', str(s or ""))
    return m.group(1) if m else str(s or "")


def _parse_manifest(manifest):
    """Return (meta_dict, [(canvas_id, width, height), ...]) for a IIIF 2.0 manifest."""
    meta = {}
    for item in manifest.get("metadata", []):
        label = _meta_label(item)
        if label and label not in meta:
            meta[label] = _meta_first(item)

    canvases = []
    for canvas in manifest.get("sequences", [{}])[0].get("canvases", []):
        m = re.search(r"/canvas/(\d+)", canvas.get("@id", ""))
        if m:
            canvases.append((int(m.group(1)),
                             canvas.get("width"), canvas.get("height")))

    return {
        "title":  meta.get("Title", "") or str(manifest.get("label", "")),
        "date":   meta.get("Date", ""),
        "scale":  meta.get("Scale", ""),
        "handle": _extract_href(meta.get("Permalink", "")),
    }, canvases


def fetch_map_metadata(conn, pid):
    """Fetch the IIIF manifest for one map and update the DB. True on success.
    Status is only ever advanced discovered->ready, never downgraded, so this is
    safe to run over already-downloaded/georeferenced maps just to fill titles."""
    manifest = fetch_json(MANIFEST_URL.format(pid=pid))
    if manifest is None:
        # Only flag maps that had nothing yet; don't clobber advanced records.
        conn.execute("UPDATE maps SET status='failed', notes='manifest_fetch_failed' "
                     "WHERE map_pid=? AND status='discovered'", (pid,))
        conn.commit()
        return False

    meta, canvases = _parse_manifest(manifest)
    if not canvases:
        conn.execute("UPDATE maps SET status='failed', notes='no_canvas_in_manifest' "
                     "WHERE map_pid=? AND status='discovered'", (pid,))
        conn.commit()
        return False

    canvas_id, w, h = canvases[0]   # tithe maps are single-canvas
    conn.execute(
        """UPDATE maps SET title=?, date=?, scale=?, handle_url=?,
           canvas_id=?, width=?, height=?,
           status=CASE WHEN status='discovered' THEN 'ready' ELSE status END
           WHERE map_pid=?""",
        (meta["title"], meta["date"], meta["scale"], meta["handle"],
         canvas_id, w, h, pid),
    )
    conn.commit()
    logging.info(f"  Metadata for {pid}: {meta['title'][:60]}  "
                 f"canvas={canvas_id} {w}x{h}px")
    return True


def cmd_metadata(args):
    conn = get_conn()
    # Fetch for any map still missing a readable title (covers discovered maps
    # and any downloaded/georeferenced ones registered without one).
    q = "SELECT map_pid FROM maps WHERE title IS NULL"
    params = []
    if args.county:
        q += " AND county=?"
        params.append(args.county)
    if args.pid:
        q = "SELECT map_pid FROM maps WHERE map_pid=?"
        params = [args.pid]
    q += " ORDER BY county, parish"
    rows = conn.execute(q, params).fetchall()
    if args.limit:
        rows = rows[:args.limit]

    logging.info(f"Fetching metadata for {len(rows)} maps...")
    for i, row in enumerate(rows, 1):
        logging.info(f"[{i}/{len(rows)}]")
        fetch_map_metadata(conn, row["map_pid"])
        time.sleep(API_DELAY)

    conn.close()
    logging.info("Metadata phase complete.")


# ──────────────────────────────────────────────────────────────────────────────
# Tile download + stitch
# ──────────────────────────────────────────────────────────────────────────────

def _download_tile(url, path):
    for attempt in range(MAX_RETRIES):
        data = fetch_bytes(url)
        if data:
            try:
                Image.open(BytesIO(data)).verify()
                path.write_bytes(data)
                return True
            except Exception:
                pass
        time.sleep(TILE_DELAY * (attempt + 1))
    logging.warning(f"  Tile failed permanently: {url}")
    return False


def download_canvas(canvas_id, out_path, tiles_dir, keep_tiles=False, scale=1):
    """Download all tiles for a IIIF canvas and stitch into one JPEG.
    Resume-safe: existing tiles are skipped. Returns True on success.

    scale: 1 = native resolution; 2/4/8 = downscale by that factor
    (the server does the resampling, so bandwidth shrinks too)."""
    info = fetch_json(INFO_URL.format(canvas_id=canvas_id))
    if not info:
        logging.error(f"  No info.json for canvas {canvas_id}")
        return False

    w, h = info["width"], info["height"]
    tiles = info.get("tiles", [])
    if not tiles:
        logging.error("  No tile definition in info.json")
        return False
    ts = tiles[0]["width"]
    cols, rows = math.ceil(w / ts), math.ceil(h / ts)
    ots = ts // scale                      # output tile size
    ow, oh = math.ceil(w / scale), math.ceil(h / scale)
    size = f"{ts}," if scale == 1 else f"pct:{100 / scale:g}"
    logging.info(f"  Canvas {canvas_id}: {w}x{h}px, {cols}x{rows} tiles of {ts}px"
                 + (f", downscaling x{scale} -> {ow}x{oh}px" if scale > 1 else ""))

    tiles_dir.mkdir(parents=True, exist_ok=True)
    failed = 0
    with tqdm(total=rows * cols, desc="  tiles", unit="tile", leave=False) as pbar:
        for r in range(rows):
            for c in range(cols):
                tp = tiles_dir / f"tile_{r}_{c}.jpg"
                if not tp.exists():
                    url = TILE_URL.format(canvas_id=canvas_id,
                                          x=c * ts, y=r * ts, ts=ts, size=size)
                    if not _download_tile(url, tp):
                        failed += 1
                    time.sleep(TILE_DELAY)
                pbar.update(1)

    if failed:
        logging.error(f"  {failed} tiles failed -- not stitching. Re-run to retry.")
        return False

    final = Image.new("RGB", (ow, oh), (255, 255, 255))
    for r in range(rows):
        for c in range(cols):
            tp = tiles_dir / f"tile_{r}_{c}.jpg"
            try:
                with Image.open(tp) as img:
                    final.paste(img, (c * ots, r * ots))
            except Exception as e:
                logging.error(f"  Corrupt tile {tp.name}: {e} -- aborting stitch.")
                tp.unlink(missing_ok=True)   # force re-download next run
                return False
    final.save(out_path, quality=95)
    logging.info(f"  Saved: {out_path.name}")

    if not keep_tiles:
        shutil.rmtree(tiles_dir, ignore_errors=True)
    return True


# ──────────────────────────────────────────────────────────────────────────────
# PARCELS -- fetch all apportionment parcels for a map, write GeoJSON points
# ──────────────────────────────────────────────────────────────────────────────

def fetch_parcels(map_pid):
    """Return the full list of parcel features for one map from the /points API."""
    features = []
    page = 1
    while True:
        url = f"{POINTS_API}?rows={ROWS_PER_PAGE}&page={page}&alt=map_parent_pid:{map_pid}"
        data = fetch_json(url)
        if data is None:
            logging.warning(f"  Parcel page {page} failed -- result may be incomplete.")
            return None
        batch = data.get("features", [])
        features.extend(batch)
        if len(batch) < ROWS_PER_PAGE:
            break
        page += 1
        time.sleep(API_DELAY)
    return features


def viewer_url(map_pid):
    """Online NLW viewer URL for a map (the bare form, no viewport fragment)."""
    return VIEWER_URL.format(pid=map_pid)


def _coverage_hectares(features):
    """Ground area (hectares) of the convex hull of a map's parcel points.

    A proxy for how much land the map covers, robust to the odd stray point in
    a way a plain bounding box is not. Uses the same local equirectangular
    approximation as the georeferencing residuals (good to ~0.1% at this scale).
    Returns None for fewer than 3 distinct points (area is undefined, not zero).
    """
    pts = []
    for f in features:
        geom = f.get("geometry") or {}
        c = geom.get("coordinates")
        if c and len(c) >= 2:
            pts.append((float(c[0]), float(c[1])))     # (lng, lat)
    if len(pts) < 3:
        return None

    mean_lat = sum(p[1] for p in pts) / len(pts)
    M_PER_DEG = 111_320.0
    k = math.cos(math.radians(mean_lat))
    proj = sorted(set((lng * M_PER_DEG * k, lat * M_PER_DEG) for lng, lat in pts))
    if len(proj) < 3:
        return None

    # Andrew's monotone-chain convex hull (dependency-free)
    def cross(o, a, b):
        return (a[0]-o[0]) * (b[1]-o[1]) - (a[1]-o[1]) * (b[0]-o[0])

    lower = []
    for p in proj:
        while len(lower) >= 2 and cross(lower[-2], lower[-1], p) <= 0:
            lower.pop()
        lower.append(p)
    upper = []
    for p in reversed(proj):
        while len(upper) >= 2 and cross(upper[-2], upper[-1], p) <= 0:
            upper.pop()
        upper.append(p)
    hull = lower[:-1] + upper[:-1]
    if len(hull) < 3:
        return None

    # Shoelace area (m^2) -> hectares
    area2 = 0.0
    for i in range(len(hull)):
        x1, y1 = hull[i]
        x2, y2 = hull[(i + 1) % len(hull)]
        area2 += x1 * y2 - x2 * y1
    return round(abs(area2) / 2.0 / 10_000.0, 2)


def write_parcels_geojson(path, row, features, scale=1):
    """Write parcel points as GeoJSON: WGS84 geometry + pixel coords + attributes.
    pixel_x/pixel_y are scaled to match the downloaded image resolution."""
    out_features = []
    for f in features:
        props = dict(f.get("properties", {}))
        px, py = parcel_pixel_centre(props)
        props["pixel_x"] = px // scale if px is not None else None
        props["pixel_y"] = py // scale if py is not None else None
        props.pop("coordinates", None)   # duplicate of the geometry
        out_features.append({
            "type": "Feature",
            "geometry": f.get("geometry"),
            "properties": props,
        })

    fc = {
        "type": "FeatureCollection",
        "name": f"parcels_{row['map_pid']}",
        "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}},
        "metadata": {
            "map_pid": row["map_pid"],
            "app_pid": row["app_pid"],
            "county": row["county"],
            "parish": row["parish"],
            "scale_factor": scale,
            "pixel_coords_note": "pixel_x/pixel_y are parcel centres on the "
                                 "downloaded map image (SAM point prompts); "
                                 "already divided by scale_factor",
            "generated": datetime.now().isoformat(),
        },
        "features": out_features,
    }
    path.write_text(json.dumps(fc, ensure_ascii=False), encoding="utf-8")
    logging.info(f"  Saved: {path.name}  ({len(out_features)} parcel points)")


def load_or_fetch_parcels(row, geojson_path, refetch=False):
    """Return parcel features, preferring the cached GeoJSON unless refetch."""
    if geojson_path.exists() and not refetch:
        logging.info(f"  Re-using cached {geojson_path.name}")
        fc = json.loads(geojson_path.read_text(encoding="utf-8"))
        return fc.get("features", []), False
    features = fetch_parcels(row["map_pid"])
    return features, True


# ──────────────────────────────────────────────────────────────────────────────
# GEOREFERENCE -- polynomial fit from parcel GCPs, VRT + world file output
# ──────────────────────────────────────────────────────────────────────────────

def gcps_from_features(features, scale=1):
    """(pixel_x, pixel_y, lng, lat) for every parcel with both coordinate sets.
    Works on raw API features and on cached GeoJSON features alike.
    Cached features carry image-space pixel_x/pixel_y already; only coords
    derived fresh from map_coords (canvas space) need dividing by scale."""
    gcps = []
    seen = set()
    for f in features:
        props = f.get("properties", {})
        geom = f.get("geometry") or {}
        coords = geom.get("coordinates")
        px = props.get("pixel_x")
        py = props.get("pixel_y")
        if px is None or py is None:
            px, py = parcel_pixel_centre(props)
            if px is not None:
                px, py = px // scale, py // scale
        if px is None or not coords:
            continue
        key = (px, py)
        if key in seen:
            continue
        seen.add(key)
        gcps.append((float(px), float(py), float(coords[0]), float(coords[1])))
    return gcps


def _fit_polynomial(gcps, order):
    """Least-squares pixel->WGS84 fit. order 1 = affine, 2 = quadratic."""
    import numpy as np

    def design(px, py):
        if order == 1:
            return [1.0, px, py]
        return [1.0, px, py, px * px, px * py, py * py]

    A = np.array([design(px, py) for px, py, _, _ in gcps])
    lng_c, *_ = np.linalg.lstsq(A, np.array([g[2] for g in gcps]), rcond=None)
    lat_c, *_ = np.linalg.lstsq(A, np.array([g[3] for g in gcps]), rcond=None)
    return lng_c, lat_c, design


def _residuals_m(gcps, lng_c, lat_c, design):
    import numpy as np
    M_PER_DEG = 111_320.0
    out = []
    for px, py, lng, lat in gcps:
        row = np.array(design(px, py))
        dlng = (float(row @ lng_c) - lng) * M_PER_DEG * math.cos(math.radians(lat))
        dlat = (float(row @ lat_c) - lat) * M_PER_DEG
        out.append(math.hypot(dlng, dlat))
    return out


def _sigma_clip(gcps, sigma=3.0):
    import numpy as np
    lng_c, lat_c, design = _fit_polynomial(gcps, order=1)
    res = np.array(_residuals_m(gcps, lng_c, lat_c, design))
    thresh = res.mean() + sigma * res.std()
    kept = [g for g, r in zip(gcps, res) if r <= thresh]
    if len(kept) < len(gcps):
        logging.info(f"  Sigma-clip removed {len(gcps)-len(kept)} outlier GCPs "
                     f"(threshold {thresh:.0f} m).")
    return kept


_WGS84_PRJ = (
    'GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",'
    'SPHEROID["WGS_1984",6378137.0,298.257223563]],'
    'PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]]'
)
_WGS84_WKT = (
    'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],'
    'PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433,'
    'AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]'
)
_WLD_EXT = {".jpg": ".jgw", ".jpeg": ".jgw", ".tif": ".tfw",
            ".tiff": ".tfw", ".png": ".pgw"}


def _write_world_file(path, lng_c, lat_c, design):
    import numpy as np
    A, D = float(lng_c[1]), float(lat_c[1])
    B, E = float(lng_c[2]), float(lat_c[2])
    row = np.array(design(0.5, 0.5))
    C, F = float(row @ lng_c), float(row @ lat_c)
    path.write_text(f"{A:.10f}\n{D:.10f}\n{B:.10f}\n{E:.10f}\n{C:.10f}\n{F:.10f}\n")


def _write_vrt(vrt_path, img_path, gcps=None, geotransform=None, projection=None):
    """GDAL VRT wrapper for the map image.

    geotransform -> affine VRT: QGIS displays it georeferenced immediately.
    gcps         -> GCP VRT: pixel-space in QGIS, but gdalwarp -tps turns it
                    into a precisely warped GeoTIFF.
    projection   -> WKT for the GCPs (defaults to WGS84).
    """
    wkt = projection or _WGS84_WKT
    with Image.open(img_path) as im:
        w, h, mode = im.width, im.height, im.mode
        bands = len(im.getbands())

    color = {"RGBA": ["Red", "Green", "Blue", "Alpha"],
             "RGB":  ["Red", "Green", "Blue"],
             "L":    ["Gray"]}.get(mode, ["Undefined"] * bands)

    lines = [f'<VRTDataset rasterXSize="{w}" rasterYSize="{h}">']
    if geotransform:
        wkt_escaped = wkt.replace("<", "&lt;").replace(">", "&gt;")
        lines.append(f'  <SRS>{wkt_escaped}</SRS>')
        gt = ", ".join(f"{v:.12g}" for v in geotransform)
        lines.append(f'  <GeoTransform>{gt}</GeoTransform>')
    if gcps:
        proj = wkt.replace('"', "&quot;")
        lines.append(f'  <GCPList Projection="{proj}">')
        for i, (px, py, lng, lat) in enumerate(gcps, 1):
            lines.append(f'    <GCP Id="{i}" Pixel="{px:.1f}" Line="{py:.1f}" '
                         f'X="{lng:.8f}" Y="{lat:.8f}"/>')
        lines.append("  </GCPList>")
    for b in range(1, bands + 1):
        lines += [
            f'  <VRTRasterBand dataType="Byte" band="{b}">',
            f'    <ColorInterp>{color[b-1] if b <= len(color) else "Undefined"}</ColorInterp>',
            '    <SimpleSource>',
            f'      <SourceFilename relativeToVRT="1">{img_path.name}</SourceFilename>',
            f'      <SourceBand>{b}</SourceBand>',
            f'      <SrcRect xOff="0" yOff="0" xSize="{w}" ySize="{h}"/>',
            f'      <DstRect xOff="0" yOff="0" xSize="{w}" ySize="{h}"/>',
            '    </SimpleSource>',
            '  </VRTRasterBand>',
        ]
    lines.append("</VRTDataset>")
    vrt_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _find_gdal_tool(name):
    """Locate a GDAL exe -- QGIS and OSGeo4W installs bundle them."""
    for pattern in (rf"C:\Program Files\QGIS*\bin\{name}.exe",
                    rf"C:\OSGeo4W*\bin\{name}.exe"):
        hits = sorted(glob.glob(pattern))
        if hits:
            return Path(hits[-1])
    found = shutil.which(name)
    return Path(found) if found else None


def _gdal_env(tool: Path):
    env = os.environ.copy()
    root = tool.parent.parent
    for cand in (root / "share" / "proj", root / "apps" / "proj" / "share" / "proj"):
        if cand.exists():
            env["PROJ_LIB"] = str(cand)
            break
    for cand in (root / "share" / "gdal", root / "apps" / "gdal" / "share" / "gdal"):
        if cand.exists():
            env["GDAL_DATA"] = str(cand)
            break
    return env


def build_overviews(tif_path: Path):
    """Add raster pyramids so QGIS can display huge images without choking."""
    gdaladdo = _find_gdal_tool("gdaladdo")
    if not gdaladdo:
        logging.warning("  gdaladdo not found -- skipping overviews "
                        "(build pyramids in QGIS layer properties instead).")
        return False
    logging.info("  Building overviews (pyramids)...")
    result = subprocess.run(
        [str(gdaladdo), "-r", "average", str(tif_path), "2", "4", "8", "16", "32", "64"],
        env=_gdal_env(gdaladdo), capture_output=True, text=True)
    if result.returncode != 0:
        logging.error(f"  gdaladdo failed: {result.stderr.strip()[-400:]}")
        return False
    logging.info("  Overviews built -- QGIS will render this smoothly.")
    return True


def warp_geotiff(gcps_vrt: Path, out_tif: Path):
    """North-up 2nd-order polynomial warp via the QGIS-bundled gdalwarp,
    plus overview pyramids. Returns True on success."""
    gdalwarp = _find_gdal_tool("gdalwarp")
    if not gdalwarp:
        logging.warning("  gdalwarp not found (install QGIS/OSGeo4W) -- skipping warp.")
        return False

    cmd = [str(gdalwarp), "-overwrite", "-order", "2",
           "-t_srs", "EPSG:4326", "-r", "bilinear",
           "-co", "COMPRESS=DEFLATE", "-co", "TILED=YES",
           "-co", "BIGTIFF=IF_SAFER",
           str(gcps_vrt), str(out_tif)]
    logging.info(f"  Warping with {gdalwarp.name} (this can take several minutes)...")
    result = subprocess.run(cmd, env=_gdal_env(gdalwarp), capture_output=True, text=True)
    if result.returncode != 0:
        logging.error(f"  gdalwarp failed: {result.stderr.strip()[-400:]}")
        return False
    logging.info(f"  Saved: {out_tif.name} (north-up georeferenced GeoTIFF)")
    build_overviews(out_tif)
    return True


def process_parcels_and_georef(conn, row, refetch=False, georef=True, warp=False):
    """Write the parcels GeoJSON and (optionally) VRT + world file for one map.
    Returns (success, api_was_hit) so callers can skip the rate-limit delay
    for fully cached maps."""
    import numpy as np

    # Anchor all outputs next to the image if it exists (the VRT references
    # the image by relative filename, so they must share a directory).
    img_path = Path(row["image_path"]) if row["image_path"] else None
    if img_path and img_path.exists():
        folder, stem = img_path.parent, img_path.stem
    else:
        folder, stem = map_paths(row)
        img_path = folder / f"{stem}.jpg"
    folder.mkdir(parents=True, exist_ok=True)
    geojson_path = folder / f"{stem}.parcels.geojson"

    keys = row.keys()
    scale = (row["scale_factor"] if "scale_factor" in keys else 1) or 1

    features, fetched = load_or_fetch_parcels(row, geojson_path, refetch)
    if features is None:
        logging.error("  Parcel fetch failed -- skipping.")
        return False, True
    if fetched:
        write_parcels_geojson(geojson_path, row, features, scale=scale)
        conn.execute(
            "UPDATE maps SET parcels_path=?, coverage_hectares=? WHERE map_pid=?",
            (str(geojson_path), _coverage_hectares(features), row["map_pid"]),
        )
        conn.commit()

    if not georef:
        return True, fetched

    if not img_path.exists():
        logging.warning("  Image not on disk -- parcels written, georeferencing skipped.")
        return True, fetched

    gcps = gcps_from_features(features, scale=scale)
    if len(gcps) < MIN_GCPS:
        logging.warning(f"  Only {len(gcps)} usable GCPs (need {MIN_GCPS}) -- "
                        "georeferencing skipped.")
        return True, fetched
    gcps = _sigma_clip(gcps)
    if len(gcps) < MIN_GCPS:
        logging.warning("  Too few GCPs after outlier removal -- georeferencing skipped.")
        return True, fetched

    lng1, lat1, d1 = _fit_polynomial(gcps, order=1)
    rms1 = float(np.sqrt(np.mean(np.array(_residuals_m(gcps, lng1, lat1, d1)) ** 2)))
    lng2, lat2, d2 = _fit_polynomial(gcps, order=2)
    rms2 = float(np.sqrt(np.mean(np.array(_residuals_m(gcps, lng2, lat2, d2)) ** 2)))
    logging.info(f"  {len(gcps)} GCPs | affine RMS {rms1:.0f} m | quadratic RMS {rms2:.0f} m")

    # GDAL geotransform from the affine fit: world = c0 + c1*px + c2*py,
    # with pixel (0,0) at the top-left image corner.
    geotransform = (float(lng1[0]), float(lng1[1]), float(lng1[2]),
                    float(lat1[0]), float(lat1[1]), float(lat1[2]))
    _write_vrt(folder / f"{stem}.vrt", img_path, geotransform=geotransform)
    _write_vrt(folder / f"{stem}.gcps.vrt", img_path, gcps=gcps)
    wld_ext = _WLD_EXT.get(img_path.suffix.lower(), ".wld")
    _write_world_file(folder / f"{stem}{wld_ext}", lng1, lat1, d1)
    (folder / f"{stem}.prj").write_text(_WGS84_PRJ)
    logging.info(f"  Saved: {stem}.vrt (open THIS in QGIS -- displays georeferenced)")
    logging.info(f"  Saved: {stem}.gcps.vrt (for precise TPS warp: "
                 f"gdalwarp -tps -t_srs EPSG:4326 {stem}.gcps.vrt {stem}_warped.tif)")
    logging.info(f"  Saved: {stem}{wld_ext} + {stem}.prj (world-file fallback)")

    if warp:
        warped = folder / f"{stem}_warped.tif"
        if warped.exists():
            logging.info(f"  {warped.name} already exists -- skipping warp.")
        else:
            warp_geotiff(folder / f"{stem}.gcps.vrt", warped)

    conn.execute(
        "UPDATE maps SET status='georeferenced', georef_rms_m=? WHERE map_pid=?",
        (round(rms2, 1), row["map_pid"]),
    )
    conn.commit()
    return True, fetched


# ──────────────────────────────────────────────────────────────────────────────
# DOWNLOAD -- map image, then parcels + georeferencing, per record
# ──────────────────────────────────────────────────────────────────────────────

def _resolve_targets(conn, specs):
    """Resolve a list of PIDs and/or parish names to map PIDs.
    Names are matched case-insensitively, exact first, then substring."""
    pids = []
    for s in specs:
        s = s.strip()
        if not s or s.startswith("#"):
            continue
        if s.isdigit():
            pids.append(int(s))
            continue
        norm = _norm(s)
        rows = conn.execute(
            f"SELECT map_pid, county, parish FROM maps "
            f"WHERE {_norm_sql('parish')}=?", (norm,)).fetchall()
        if not rows:
            rows = conn.execute(
                f"SELECT map_pid, county, parish FROM maps "
                f"WHERE {_norm_sql('parish')} LIKE ?", (f"%{norm}%",)).fetchall()
        if not rows:
            logging.warning(f"No map in the database matches '{s}' "
                            "(run 'discover' first, or check spelling with 'list').")
        elif len(rows) > 1:
            opts = ", ".join(f"{r['parish']} ({r['county']}, pid={r['map_pid']})"
                             for r in rows)
            logging.warning(f"'{s}' is ambiguous -- use a PID. Matches: {opts}")
        else:
            pids.append(rows[0]["map_pid"])
    return pids


def _resolve_geojson_path(row):
    """Locate a map's .parcels.geojson: the stored path, else the conventional
    location beside its image / in the county folder."""
    if row["parcels_path"]:
        p = Path(row["parcels_path"])
        if p.exists():
            return p
    img = Path(row["image_path"]) if row["image_path"] else None
    if img and img.exists():
        cand = img.parent / f"{img.stem}.parcels.geojson"
    else:
        folder, stem = map_paths(row)
        cand = folder / f"{stem}.parcels.geojson"
    return cand if cand.exists() else None


def backfill_coverage(conn, force=False):
    """Populate coverage_hectares for maps whose parcel points are on disk.
    Returns the number of maps updated. Idempotent; skips already-computed
    maps unless force=True."""
    where = "" if force else "WHERE coverage_hectares IS NULL"
    rows = conn.execute(f"SELECT * FROM maps {where}").fetchall()
    updated = 0
    for row in rows:
        gj = _resolve_geojson_path(row)
        if not gj:
            continue
        try:
            fc = json.loads(gj.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        cov = _coverage_hectares(fc.get("features", []))
        conn.execute(
            "UPDATE maps SET coverage_hectares=?, parcels_path=COALESCE(parcels_path,?) "
            "WHERE map_pid=?", (cov, str(gj), row["map_pid"]))
        updated += 1
        if updated % 100 == 0:
            conn.commit()
            logging.info(f"  coverage computed for {updated} maps...")
    conn.commit()
    return updated


def cmd_coverage(args):
    conn = get_conn()
    logging.info("Computing coverage area from parcel points "
                 "(reads cached .parcels.geojson files)...")
    n = backfill_coverage(conn, force=args.force)
    total = conn.execute(
        "SELECT COUNT(*) FROM maps WHERE coverage_hectares IS NOT NULL").fetchone()[0]
    logging.info(f"Coverage populated for {n} map(s) this run; "
                 f"{total} maps now have a coverage figure.")
    conn.close()


def cmd_list(args):
    conn = get_conn()

    # Sorting by area needs coverage_hectares; backfill any missing on demand.
    sort = getattr(args, "sort", None)
    if sort == "area":
        missing = conn.execute(
            "SELECT COUNT(*) FROM maps WHERE coverage_hectares IS NULL").fetchone()[0]
        if missing:
            logging.info(f"Computing coverage area for up to {missing} map(s) "
                         "from cached points (one-off; cached afterwards)...")
            backfill_coverage(conn)

    conditions, params = [], []
    if args.county:
        conditions.append("county=? COLLATE NOCASE")
        params.append(args.county)
    if args.search:
        term = f"%{_norm(args.search)}%"
        conditions.append(f"({_norm_sql('parish')} LIKE ? OR {_norm_sql('title')} LIKE ?)")
        params += [term, term]
    if args.status:
        conditions.append("status=?")
        params.append(args.status)
    where = " AND ".join(conditions) if conditions else "1=1"

    order = {
        None:      "county, parish",
        "county":  "county, parish",
        "parcels": "parcel_count ASC, county, parish",
        "area":    "coverage_hectares IS NULL, coverage_hectares ASC, county, parish",
    }[sort]
    if getattr(args, "desc", False) and sort in ("parcels", "area"):
        col = "parcel_count" if sort == "parcels" else "coverage_hectares"
        order = f"{col} DESC, county, parish"

    limit_sql = f" LIMIT {int(args.limit)}" if getattr(args, "limit", None) else ""
    rows = conn.execute(
        f"SELECT map_pid, county, parish, parcel_count, coverage_hectares, "
        f"quality, status FROM maps WHERE {where} "
        f"ORDER BY {order}{limit_sql}", params).fetchall()

    show_url = getattr(args, "urls", False)
    print(f"\n{'PID':>9}  {'County':<12} {'Parish':<26} {'Parcels':>7} "
          f"{'Area/ha':>9}  {'Quality':<8} {'Status':<13}"
          + ("  Viewer URL" if show_url else ""))
    print("-" * (84 + (42 if show_url else 0)))
    for r in rows:
        area = "-" if r["coverage_hectares"] is None else f"{r['coverage_hectares']:.0f}"
        line = (f"{r['map_pid']:>9}  {(r['county'] or '?'):<12} "
                f"{(r['parish'] or '?')[:26]:<26} {r['parcel_count']:>7} "
                f"{area:>9}  {(r['quality'] or '-'):<8} {r['status']:<13}")
        if show_url:
            line += "  " + viewer_url(r["map_pid"])
        print(line)
    print(f"\n{len(rows)} map(s). Add --urls for viewer links, "
          "--sort parcels|area, --limit N.")
    print('  python tithe_downloader.py download --pids "4634773,4622522"')
    conn.close()


def _quality_filter(args):
    if getattr(args, "include_low", False):
        return "(quality IS NULL OR quality IN ('high','low'))"
    return "(quality IS NULL OR quality='high')"


def cmd_download(args):
    conn = get_conn()

    # Explicit targets (--pid / --pids / --from-file) bypass the quality gate
    targets = []
    if args.pid:
        targets.append(str(args.pid))
    if args.pids:
        targets += args.pids.split(",")
    if args.from_file:
        tf = Path(args.from_file)
        if not tf.exists():
            logging.error(f"Target file not found: {tf}")
            conn.close()
            return
        targets += tf.read_text(encoding="utf-8").splitlines()

    if targets:
        pids = _resolve_targets(conn, targets)
        rows = [conn.execute("SELECT * FROM maps WHERE map_pid=?", (p,)).fetchone()
                for p in pids]
        rows = [r for r in rows if r]
    else:
        conditions = ["status IN ('ready','downloaded')", _quality_filter(args)]
        params = []
        if args.county:
            conditions.append("county=?")
            params.append(args.county)
        if args.min_parcels:
            conditions.append("parcel_count>=?")
            params.append(args.min_parcels)
        rows = conn.execute(
            f"SELECT * FROM maps WHERE {' AND '.join(conditions)} "
            "ORDER BY county, parish", params).fetchall()

    if args.limit:
        rows = rows[:args.limit]

    if not rows:
        logging.info("Nothing to download. (Run 'discover' first? Check quality flags?)")
        conn.close()
        return

    logging.info(f"{len(rows)} map(s) to process.")
    for i, row in enumerate(rows, 1):
        pid = row["map_pid"]

        # Fetch metadata on demand for targeted maps not yet processed
        if row["canvas_id"] is None:
            if not fetch_map_metadata(conn, pid):
                logging.warning(f"  Metadata fetch failed for {pid} -- skipping.")
                continue
            row = conn.execute("SELECT * FROM maps WHERE map_pid=?", (pid,)).fetchone()
            time.sleep(API_DELAY)

        # Respect an already-registered image location (e.g. legacy downloads)
        if row["image_path"] and Path(row["image_path"]).exists():
            img_path = Path(row["image_path"])
            folder = img_path.parent
        else:
            folder, stem = map_paths(row)
            img_path = folder / f"{stem}.jpg"
        logging.info(f"\n[{i}/{len(rows)}] {row['county']} / {row['parish']} (pid={pid})")

        # 1. Map image
        if img_path.exists():
            logging.info("  Image already downloaded.")
            if not row["image_path"]:
                conn.execute("UPDATE maps SET image_path=?, status='downloaded' "
                             "WHERE map_pid=?", (str(img_path), pid))
                conn.commit()
        else:
            folder.mkdir(parents=True, exist_ok=True)
            ok = download_canvas(row["canvas_id"], img_path, folder / ".tiles",
                                 keep_tiles=args.keep_tiles, scale=args.scale)
            if not ok:
                conn.execute("UPDATE maps SET notes='download_failed' WHERE map_pid=?",
                             (pid,))
                conn.commit()
                continue
            conn.execute(
                "UPDATE maps SET status='downloaded', image_path=?, "
                "scale_factor=?, downloaded_date=? WHERE map_pid=?",
                (str(img_path), args.scale, datetime.now().isoformat(), pid),
            )
            conn.commit()

        # 2. Parcel points + georeferencing
        if not args.image_only:
            row = conn.execute("SELECT * FROM maps WHERE map_pid=?", (pid,)).fetchone()
            process_parcels_and_georef(conn, row, warp=args.warp)

        time.sleep(API_DELAY)

    conn.close()
    logging.info("\nDownload session complete.")


# ──────────────────────────────────────────────────────────────────────────────
# PARCELS / GEOREFERENCE as standalone commands
# ──────────────────────────────────────────────────────────────────────────────

def _select_rows(conn, args, need_image=False):
    conditions, params = [], []
    if args.pid:
        conditions.append("map_pid=?")
        params.append(args.pid)
    else:
        if need_image:
            conditions.append("status IN ('downloaded','georeferenced')")
        if args.county:
            conditions.append("county=?")
            params.append(args.county)
    where = " AND ".join(conditions) if conditions else "1=1"
    return conn.execute(
        f"SELECT * FROM maps WHERE {where} ORDER BY county, parish", params
    ).fetchall()


def cmd_parcels(args):
    conn = get_conn()
    rows = _select_rows(conn, args)
    logging.info(f"Building parcel files for {len(rows)} map(s)...")
    for row in rows:
        logging.info(f"\n{row['county']} / {row['parish']} (pid={row['map_pid']})")
        _, api_hit = process_parcels_and_georef(conn, row, refetch=args.refetch,
                                                georef=False)
        if api_hit:
            time.sleep(API_DELAY)
    conn.close()


def cmd_georeference(args):
    conn = get_conn()
    rows = _select_rows(conn, args, need_image=True)
    logging.info(f"Georeferencing {len(rows)} map(s)...")
    for row in rows:
        logging.info(f"\n{row['county']} / {row['parish']} (pid={row['map_pid']})")
        _, api_hit = process_parcels_and_georef(conn, row, refetch=args.refetch,
                                                warp=args.warp)
        if api_hit:
            time.sleep(API_DELAY)
    conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# GEOPACKAGE -- bundle all downloaded parcel points into GPKG file(s) for QGIS
# ──────────────────────────────────────────────────────────────────────────────
# Written with plain sqlite3 (a GeoPackage IS a SQLite file) -- no GDAL needed.

_GPKG_WGS84_WKT = (
    'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,'
    'AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],'
    'PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],'
    'UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],'
    'AUTHORITY["EPSG","4326"]]'
)

_GPKG_COLUMNS = [
    ("map_pid", "INTEGER"), ("county", "TEXT"), ("parish", "TEXT"),
    ("field_number", "TEXT"), ("farm_name", "TEXT"), ("field_name", "TEXT"),
    ("land_use", "TEXT"), ("occupier", "TEXT"), ("landowner", "TEXT"),
    ("acres", "REAL"), ("roods", "REAL"), ("perches", "REAL"),
    ("area_hectares", "REAL"),
    ("pounds", "REAL"), ("shillings", "REAL"), ("pence", "REAL"),
    ("rent_decimal_pounds", "REAL"),
    ("app_order", "INTEGER"), ("pixel_x", "INTEGER"), ("pixel_y", "INTEGER"),
    ("nlw_id", "TEXT"),
]


def _to_number(v):
    """Coerce an apportionment value to float, or None if absent/unparseable."""
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _area_hectares(acres, roods, perches):
    """Statute imperial area (1 acre = 4 roods = 160 perches) -> hectares.
    Blank components count as 0; all-blank -> None."""
    a, r, p = _to_number(acres), _to_number(roods), _to_number(perches)
    if a is None and r is None and p is None:
        return None
    total_acres = (a or 0.0) + (r or 0.0) / 4.0 + (p or 0.0) / 160.0
    return round(total_acres * 0.40468564224, 4)


def _rent_decimal_pounds(pounds, shillings, pence):
    """Pre-decimal rent-charge (1 GBP = 20s, 1s = 12d) -> decimal pounds.
    Blank components count as 0; all-blank -> None."""
    l, s, d = _to_number(pounds), _to_number(shillings), _to_number(pence)
    if l is None and s is None and d is None:
        return None
    return round((l or 0.0) + (s or 0.0) / 20.0 + (d or 0.0) / 240.0, 4)


def _gpkg_point_blob(x, y, srs_id=4326):
    """GeoPackage geometry BLOB: GP header (no envelope) + little-endian WKB point."""
    import struct
    header = struct.pack("<2sBBi", b"GP", 0, 0x01, srs_id)
    wkb = struct.pack("<BIdd", 1, 1, x, y)
    return header + wkb


def _gpkg_open(path: Path):
    """Create a new GeoPackage file with the required metadata tables."""
    try:
        path.unlink(missing_ok=True)
    except PermissionError:
        logging.error(f"{path.name} is open in another program (QGIS?). "
                      "Close it there and re-run.")
        raise SystemExit(1)
    db = sqlite3.connect(path)
    db.execute("PRAGMA application_id=0x47504B47")   # 'GPKG'
    db.execute("PRAGMA user_version=10300")          # GeoPackage 1.3
    db.executescript("""
        CREATE TABLE gpkg_spatial_ref_sys (
            srs_name TEXT NOT NULL, srs_id INTEGER PRIMARY KEY,
            organization TEXT NOT NULL, organization_coordsys_id INTEGER NOT NULL,
            definition TEXT NOT NULL, description TEXT);
        CREATE TABLE gpkg_contents (
            table_name TEXT PRIMARY KEY, data_type TEXT NOT NULL,
            identifier TEXT UNIQUE, description TEXT DEFAULT '',
            last_change DATETIME DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            min_x DOUBLE, min_y DOUBLE, max_x DOUBLE, max_y DOUBLE,
            srs_id INTEGER);
        CREATE TABLE gpkg_geometry_columns (
            table_name TEXT PRIMARY KEY, column_name TEXT NOT NULL,
            geometry_type_name TEXT NOT NULL, srs_id INTEGER NOT NULL,
            z TINYINT NOT NULL, m TINYINT NOT NULL);
    """)
    db.executemany(
        "INSERT INTO gpkg_spatial_ref_sys VALUES (?,?,?,?,?,?)",
        [("Undefined cartesian SRS", -1, "NONE", -1, "undefined", None),
         ("Undefined geographic SRS", 0, "NONE", 0, "undefined", None),
         ("WGS 84", 4326, "EPSG", 4326, _GPKG_WGS84_WKT, None)])
    return db


def _gpkg_add_layer(db, layer: str, features_with_meta):
    """Add one point layer. features_with_meta: iterable of (feature, row)."""
    cols_sql = ", ".join(f'"{n}" {t}' for n, t in _GPKG_COLUMNS)
    db.execute(f'CREATE TABLE "{layer}" '
               f'(fid INTEGER PRIMARY KEY AUTOINCREMENT, geom BLOB, {cols_sql})')

    bbox = [180.0, 90.0, -180.0, -90.0]
    records = []
    for feat, row in features_with_meta:
        geom = feat.get("geometry") or {}
        coords = geom.get("coordinates")
        if not coords:
            continue
        lng, lat = float(coords[0]), float(coords[1])
        bbox = [min(bbox[0], lng), min(bbox[1], lat),
                max(bbox[2], lng), max(bbox[3], lat)]
        p = feat.get("properties", {})
        land_use = p.get("land_use_facet")
        if isinstance(land_use, list):
            land_use = "; ".join(str(v) for v in land_use)
        records.append((
            _gpkg_point_blob(lng, lat),
            row["map_pid"], row["county"], row["parish"],
            str(p.get("field_number")) if p.get("field_number") is not None else None,
            p.get("farm_name"), p.get("field_name"), land_use,
            p.get("occupier_facet"), p.get("landowner_facet"),
            p.get("acres"), p.get("roods"), p.get("perches"),
            _area_hectares(p.get("acres"), p.get("roods"), p.get("perches")),
            p.get("pounds"), p.get("shillings"), p.get("pence"),
            _rent_decimal_pounds(p.get("pounds"), p.get("shillings"), p.get("pence")),
            p.get("app_order"), p.get("pixel_x"), p.get("pixel_y"),
            p.get("id"),
        ))
    if not records:
        db.execute(f'DROP TABLE "{layer}"')
        return 0

    ph = ",".join("?" * (len(_GPKG_COLUMNS) + 1))
    db.executemany(f'INSERT INTO "{layer}" (geom, '
                   + ", ".join(f'"{n}"' for n, _ in _GPKG_COLUMNS)
                   + f") VALUES ({ph})", records)
    db.execute("INSERT INTO gpkg_contents (table_name, data_type, identifier, "
               "min_x, min_y, max_x, max_y, srs_id) VALUES (?,?,?,?,?,?,?,4326)",
               (layer, "features", layer, *bbox))
    db.execute("INSERT INTO gpkg_geometry_columns VALUES (?,?,?,4326,0,0)",
               (layer, "geom", "POINT"))
    return len(records)


# ──────────────────────────────────────────────────────────────────────────────
# EXPORT-TOOLKIT -- hand a sheet to the Cadastral Vectorisation Toolkit
# ──────────────────────────────────────────────────────────────────────────────
# The toolkit expects, per sheet:
#   data/raw/<SHEET>/<SHEET>.tif        north-up GeoTIFF, EPSG:27700, 0.5 m/px
#   data/parcel_points/<SHEET>_points.gpkg
#                                       single point layer, EPSG:27700, with a
#                                       `rowid` column used for the attribute join
#
# CRITICAL -- how the points are positioned.
# The parcel points are NOT written from their WGS84 lon/lat.  The polynomial
# georeferencing fit has 6-56 m of residual scatter, which at 0.5 m/px is up to
# ~110 px -- easily enough to drop a watershed seed inside the WRONG parcel.
# Instead each point is placed by pushing its *pixel* position (pixel_x/pixel_y,
# i.e. NLW's own record of where that parcel sits on the scan) through the SAME
# GCP transform gdalwarp uses to warp the image, via gdaltransform.  The seed
# therefore lands exactly where that pixel landed in the output raster, and the
# residual error cancels out entirely.

TOOLKIT_EPSG = 27700
TOOLKIT_RES = 0.5    # metres per pixel

# Map our column names onto the toolkit's existing (Holnicote) schema so the
# same QGIS styles and any downstream field references keep working.
_TOOLKIT_COLUMNS = [
    # (output name, source property in our GeoJSON)
    ("ParishName",       None),            # filled from the DB row
    ("ParcelID",         "field_number"),
    ("Landowner",        "landowner_facet"),
    ("Occupier",         "occupier_facet"),
    ("FieldName_Desc",   "field_name"),
    ("AreaName",         "farm_name"),
    ("CultivationState", "land_use_facet"),
    ("Acres",            "acres"),
    ("Rods",             "roods"),         # toolkit spells roods "Rods"
    ("Perches",          "perches"),
    ("Pounds",           "pounds"),
    ("Shillings",        "shillings"),
    ("Pence",            "pence"),
]


def _gdal_batch_transform(tool: Path, args: list, coords, chunk=20000):
    """Run gdaltransform over many coordinate pairs; returns [(x, y), ...]."""
    out = []
    for i in range(0, len(coords), chunk):
        block = coords[i:i + chunk]
        stdin = "\n".join(f"{x} {y}" for x, y in block) + "\n"
        res = subprocess.run([str(tool)] + args, input=stdin, text=True,
                             capture_output=True, env=_gdal_env(tool))
        if res.returncode != 0:
            raise RuntimeError(f"gdaltransform failed: {res.stderr.strip()[-400:]}")
        for line in res.stdout.strip().splitlines():
            parts = line.split()
            out.append((float(parts[0]), float(parts[1])))
    if len(out) != len(coords):
        raise RuntimeError(
            f"gdaltransform returned {len(out)} points for {len(coords)} inputs")
    return out


def _write_points_gpkg(path: Path, layer: str, records, columns):
    """Write a single-layer point GeoPackage in EPSG:27700 (pure sqlite3)."""
    db = _gpkg_open(path)
    db.execute(
        "INSERT INTO gpkg_spatial_ref_sys VALUES (?,?,?,?,?,?)",
        ("OSGB 1936 / British National Grid", TOOLKIT_EPSG, "EPSG", TOOLKIT_EPSG,
         _BNG_WKT, None))

    col_sql = ", ".join(f'"{n}" {t}' for n, t in columns)
    db.execute(f'CREATE TABLE "{layer}" '
               f'(fid INTEGER PRIMARY KEY AUTOINCREMENT, geom BLOB, {col_sql})')

    bbox = [1e12, 1e12, -1e12, -1e12]
    rows = []
    for east, north, values in records:
        bbox = [min(bbox[0], east), min(bbox[1], north),
                max(bbox[2], east), max(bbox[3], north)]
        rows.append((_gpkg_point_blob(east, north, srs_id=TOOLKIT_EPSG), *values))

    ph = ",".join("?" * (len(columns) + 1))
    db.executemany(
        f'INSERT INTO "{layer}" (geom, ' + ", ".join(f'"{n}"' for n, _ in columns)
        + f") VALUES ({ph})", rows)
    db.execute("INSERT INTO gpkg_contents (table_name, data_type, identifier, "
               "min_x, min_y, max_x, max_y, srs_id) VALUES (?,?,?,?,?,?,?,?)",
               (layer, "features", layer, *bbox, TOOLKIT_EPSG))
    db.execute("INSERT INTO gpkg_geometry_columns VALUES (?,?,?,?,0,0)",
               (layer, "geom", "POINT", TOOLKIT_EPSG))
    db.commit()
    db.close()


_BNG_WKT = (
    'PROJCS["OSGB 1936 / British National Grid",'
    'GEOGCS["OSGB 1936",DATUM["OSGB_1936",'
    'SPHEROID["Airy 1830",6377563.396,299.3249646,AUTHORITY["EPSG","7001"]],'
    'AUTHORITY["EPSG","6277"]],PRIMEM["Greenwich",0],'
    'UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4277"]],'
    'PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",49],'
    'PARAMETER["central_meridian",-2],PARAMETER["scale_factor",0.9996012717],'
    'PARAMETER["false_easting",400000],PARAMETER["false_northing",-100000],'
    'UNIT["metre",1,AUTHORITY["EPSG","9001"]],AUTHORITY["EPSG","27700"]]'
)


def cmd_export_toolkit(args):
    """Export sheets as toolkit-ready GeoTIFF + parcel points (EPSG:27700)."""
    gdalwarp = _find_gdal_tool("gdalwarp")
    gdaltransform = _find_gdal_tool("gdaltransform")
    if not gdalwarp or not gdaltransform:
        logging.error("GDAL tools not found. Install QGIS or OSGeo4W.")
        return

    toolkit = Path(args.toolkit_dir).expanduser()
    if not (toolkit / "config.yaml").exists():
        logging.error(f"Not a toolkit directory (no config.yaml): {toolkit}")
        return
    raw_dir = toolkit / "data" / "raw"
    pts_dir = toolkit / "data" / "parcel_points"

    conn = get_conn()
    rows = _select_rows(conn, args, need_image=True)
    rows = [r for r in rows if r["image_path"] and r["parcels_path"]]
    if not rows:
        logging.info("No downloaded, georeferenced maps match the filter.")
        conn.close()
        return

    logging.info(f"Exporting {len(rows)} sheet(s) to {toolkit}")
    for row in rows:
        parish = row["parish"] or f"pid_{row['map_pid']}"
        sheet = args.sheet_name or safe_name(parish)
        img = Path(row["image_path"])
        gj = Path(row["parcels_path"])
        if not img.exists() or not gj.exists():
            logging.warning(f"  {sheet}: image or parcels file missing -- skipping.")
            continue

        logging.info(f"\n[{sheet}] {row['county']} / {parish} (pid={row['map_pid']})")
        out_tif = raw_dir / sheet / f"{sheet}.tif"
        out_pts = pts_dir / f"{sheet}_points.gpkg"
        if out_tif.exists() and not args.overwrite:
            logging.info(f"  {out_tif.relative_to(toolkit)} exists "
                         "-- use --overwrite to replace. Skipping raster.")
        out_tif.parent.mkdir(parents=True, exist_ok=True)
        pts_dir.mkdir(parents=True, exist_ok=True)

        fc = json.loads(gj.read_text(encoding="utf-8"))
        features = fc.get("features", [])
        scale = (fc.get("metadata", {}) or {}).get("scale_factor", 1) or 1

        # ── GCPs: pixel -> BNG (reproject the lon/lat GCPs once) ─────────────
        gcps = gcps_from_features(features, scale=scale)
        if len(gcps) < MIN_GCPS:
            logging.warning(f"  Only {len(gcps)} GCPs -- skipping.")
            continue
        gcps = _sigma_clip(gcps)

        lonlat = [(g[2], g[3]) for g in gcps]
        en = _gdal_batch_transform(
            gdaltransform, ["-s_srs", "EPSG:4326", "-t_srs", f"EPSG:{TOOLKIT_EPSG}"],
            lonlat)
        gcps_bng = [(g[0], g[1], e, n) for g, (e, n) in zip(gcps, en)]

        # GCP VRT in BNG -- used for BOTH the warp and the point placement, so
        # the two are guaranteed to agree.
        vrt_bng = img.parent / f"{img.stem}.bng.gcps.vrt"
        _write_vrt(vrt_bng, img, gcps=gcps_bng, projection=_BNG_WKT)

        # ── Raster ───────────────────────────────────────────────────────────
        if not out_tif.exists() or args.overwrite:
            cmd = [str(gdalwarp), "-overwrite", "-order", "2",
                   "-t_srs", f"EPSG:{TOOLKIT_EPSG}",
                   "-tr", str(TOOLKIT_RES), str(TOOLKIT_RES),
                   "-r", "bilinear", "-co", "COMPRESS=DEFLATE",
                   "-co", "TILED=YES", "-co", "BIGTIFF=IF_SAFER",
                   str(vrt_bng), str(out_tif)]
            logging.info(f"  Warping to EPSG:{TOOLKIT_EPSG} @ {TOOLKIT_RES} m/px "
                         "(several minutes)...")
            res = subprocess.run(cmd, capture_output=True, text=True,
                                 env=_gdal_env(gdalwarp))
            if res.returncode != 0:
                logging.error(f"  gdalwarp failed: {res.stderr.strip()[-400:]}")
                continue
            logging.info(f"  Saved: {out_tif.relative_to(toolkit)}")
            build_overviews(out_tif)

        # ── Points: pixel -> BNG through the SAME GCP transform ──────────────
        pixel_pts, props_list = [], []
        for f in features:
            p = f.get("properties", {})
            px, py = p.get("pixel_x"), p.get("pixel_y")
            if px is None or py is None:
                continue
            pixel_pts.append((px, py))
            props_list.append(p)

        if not pixel_pts:
            logging.warning("  No pixel coordinates in parcels file -- "
                            "points not written.")
            vrt_bng.unlink(missing_ok=True)
            continue

        world = _gdal_batch_transform(gdaltransform, ["-order", "2", str(vrt_bng)],
                                      pixel_pts)

        columns = ([("rowid", "INTEGER")]
                   + [(n, "TEXT" if s in (None, "field_number", "farm_name",
                                          "field_name", "land_use_facet",
                                          "occupier_facet", "landowner_facet")
                       else "REAL") for n, s in _TOOLKIT_COLUMNS]
                   + [("Easting", "REAL"), ("Northing", "REAL"),
                      ("area_hectares", "REAL"), ("rent_decimal_pounds", "REAL"),
                      ("pixel_x", "INTEGER"), ("pixel_y", "INTEGER"),
                      ("map_pid", "INTEGER"), ("nlw_id", "TEXT")])

        records = []
        for i, ((east, north), p) in enumerate(zip(world, props_list), start=1):
            vals = [i]
            for name, src in _TOOLKIT_COLUMNS:
                if src is None:
                    vals.append(parish)
                    continue
                v = p.get(src)
                if isinstance(v, list):
                    v = "; ".join(str(x) for x in v)
                if name in ("ParcelID",) and v is not None:
                    v = str(v)
                vals.append(v)
            vals += [east, north,
                     _area_hectares(p.get("acres"), p.get("roods"), p.get("perches")),
                     _rent_decimal_pounds(p.get("pounds"), p.get("shillings"),
                                          p.get("pence")),
                     p.get("pixel_x"), p.get("pixel_y"), row["map_pid"],
                     p.get("id")]
            records.append((east, north, vals))

        _write_points_gpkg(out_pts, f"{sheet} Apportionment Points", records, columns)
        logging.info(f"  Saved: {out_pts.relative_to(toolkit)}  "
                     f"({len(records)} seed points, EPSG:{TOOLKIT_EPSG})")
        vrt_bng.unlink(missing_ok=True)

        logging.info(f"  >> Toolkit ready. Run:  python steps/01_patchify/"
                     f"patchify.py --sheet {sheet} --mask")

    conn.close()
    logging.info("\nExport complete.")


def _stash_layer_styles(path: Path):
    """Read QGIS styles embedded in an existing GeoPackage (layer_styles table)
    so they survive a rebuild. Returns (columns, rows) or None."""
    if not path.exists():
        return None
    try:
        db = sqlite3.connect(path)
        cur = db.execute("SELECT * FROM layer_styles")
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        db.close()
        return (cols, rows) if rows else None
    except sqlite3.Error:
        return None


def _restore_layer_styles(db, stash):
    """Re-insert stashed QGIS styles whose layer still exists. Returns count."""
    if not stash:
        return 0
    cols, rows = stash
    db.execute("""CREATE TABLE IF NOT EXISTS layer_styles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        f_table_catalog TEXT, f_table_schema TEXT, f_table_name TEXT,
        f_geometry_column TEXT, styleName TEXT, styleQML TEXT, styleSLD TEXT,
        useAsDefault BOOLEAN, description TEXT, owner TEXT, ui TEXT,
        update_time DATETIME DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')))""")
    existing = {r[0] for r in db.execute(
        "SELECT table_name FROM gpkg_contents WHERE data_type='features'")}
    i_table = cols.index("f_table_name")
    n = 0
    for row in rows:
        if row[i_table] in existing:
            db.execute(f"INSERT INTO layer_styles ({','.join(cols)}) "
                       f"VALUES ({','.join('?' * len(cols))})", row)
            n += 1
    return n


def cmd_geopackage(args):
    """Bundle every downloaded .parcels.geojson into GeoPackage file(s)."""
    conn = get_conn()
    q = "SELECT * FROM maps WHERE parcels_path IS NOT NULL"
    params = []
    if args.county:
        q += " AND county=? COLLATE NOCASE"
        params.append(args.county)
    rows = conn.execute(q + " ORDER BY county, parish", params).fetchall()
    conn.close()

    # Group maps by county
    by_county = {}
    for row in rows:
        gj = Path(row["parcels_path"])
        if not gj.exists():
            logging.warning(f"  Missing parcel file, skipping: {gj}")
            continue
        by_county.setdefault(row["county"] or "Unknown", []).append(row)

    if not by_county:
        logging.info("No downloaded parcel files found. Run 'download' or 'parcels' first.")
        return

    def load(row):
        fc = json.loads(Path(row["parcels_path"]).read_text(encoding="utf-8"))
        return [(f, row) for f in fc.get("features", [])]

    total = 0
    if args.split:
        # One GeoPackage per county
        for county, county_rows in sorted(by_county.items()):
            out = BASE_DIR / f"parcels_{safe_name(county)}.gpkg"
            stash = _stash_layer_styles(out)
            db = _gpkg_open(out)
            feats = [fr for row in county_rows for fr in load(row)]
            n = _gpkg_add_layer(db, "parcels", feats)
            _restore_layer_styles(db, stash)
            db.commit()
            db.close()
            total += n
            logging.info(f"  {out.name}: {n} points from {len(county_rows)} map(s)")
    else:
        # One GeoPackage, one layer per county
        out = BASE_DIR / "parcels.gpkg"
        stash = _stash_layer_styles(out)
        db = _gpkg_open(out)
        for county, county_rows in sorted(by_county.items()):
            feats = [fr for row in county_rows for fr in load(row)]
            n = _gpkg_add_layer(db, safe_name(county), feats)
            total += n
            logging.info(f"  Layer '{county}': {n} points from {len(county_rows)} map(s)")
        n_styles = _restore_layer_styles(db, stash)
        if n_styles:
            logging.info(f"  Preserved {n_styles} embedded QGIS style(s).")
        db.commit()
        db.close()
        mb = out.stat().st_size / 1e6
        logging.info(f"Saved: {out}  ({total} points, {mb:.1f} MB)")
        if mb > 200:
            logging.info("Tip: that is getting large -- consider 'geopackage --split' "
                         "for one file per county, and load only what you need in QGIS.")
    logging.info(f"GeoPackage export complete: {total} parcel points.")


# ──────────────────────────────────────────────────────────────────────────────
# QUALITY -- manual triage flags
# ──────────────────────────────────────────────────────────────────────────────

def cmd_quality(args):
    conn = get_conn()
    if args.set:
        if not args.pid:
            print("quality --set requires --pid")
            return
        if args.set not in ("high", "low", "excluded", "none"):
            print("quality must be one of: high, low, excluded, none")
            return
        val = None if args.set == "none" else args.set
        conn.execute("UPDATE maps SET quality=? WHERE map_pid=?", (val, args.pid))
        conn.commit()
        r = conn.execute("SELECT parish, county FROM maps WHERE map_pid=?",
                         (args.pid,)).fetchone()
        print(f"Set quality={args.set} for {r['parish']} ({r['county']}), "
              f"pid={args.pid}" if r else f"No map with pid {args.pid}")
    else:
        rows = conn.execute(
            """SELECT map_pid, county, parish, parcel_count, quality, status
               FROM maps ORDER BY quality IS NOT NULL, county, parish"""
        ).fetchall()
        print(f"\n{'PID':>9}  {'County':<12} {'Parish':<28} {'Parcels':>7} "
              f"{'Quality':<9} Status")
        print("-" * 80)
        for r in rows:
            print(f"{r['map_pid']:>9}  {(r['county'] or '?'):<12} "
                  f"{(r['parish'] or '?')[:28]:<28} {r['parcel_count']:>7} "
                  f"{(r['quality'] or '-'):<9} {r['status']}")
    conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# STATUS / EXPORT
# ──────────────────────────────────────────────────────────────────────────────

def cmd_status(args):
    conn = get_conn()
    prog = conn.execute("SELECT * FROM discovery_progress WHERE id=1").fetchone()
    total = conn.execute("SELECT COUNT(*) FROM maps").fetchone()[0]

    print("\n" + "=" * 64)
    print("  Welsh Tithe Map Downloader -- Status")
    print("=" * 64)
    print(f"  Discovery : {'complete' if prog['completed'] else 'IN PROGRESS'} "
          f"({prog['last_page']} pages, {prog['total_fetched']} parcel records)")
    print(f"  Maps      : {total}")

    by_status = conn.execute(
        "SELECT status, COUNT(*) n FROM maps GROUP BY status ORDER BY n DESC"
    ).fetchall()
    for r in by_status:
        print(f"    {r['status']:<14}: {r['n']}")

    by_quality = conn.execute(
        "SELECT COALESCE(quality,'unassessed') q, COUNT(*) n FROM maps GROUP BY q"
    ).fetchall()
    print("  Quality   : " + ", ".join(f"{r['q']}={r['n']}" for r in by_quality))

    rows = conn.execute(
        """SELECT county, COUNT(*) total,
           SUM(status IN ('downloaded','georeferenced')) dl,
           SUM(status='georeferenced') geo
           FROM maps GROUP BY county ORDER BY county"""
    ).fetchall()
    if rows:
        print(f"\n  {'County':<14} {'Maps':>5} {'Downloaded':>11} {'Georef':>7}")
        print("  " + "-" * 40)
        for r in rows:
            print(f"  {(r['county'] or 'Unknown'):<14} {r['total']:>5} "
                  f"{r['dl'] or 0:>11} {r['geo'] or 0:>7}")
    print("=" * 64 + "\n")
    conn.close()


def cmd_export(args):
    conn = get_conn()
    out = BASE_DIR / "tithe_maps.csv"
    rows = conn.execute("SELECT * FROM maps ORDER BY county, parish").fetchall()
    if not rows:
        print("Database is empty.")
        return
    fieldnames = list(rows[0].keys()) + ["viewer_url"]
    with open(out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            rec = dict(r)
            rec["viewer_url"] = viewer_url(r["map_pid"])   # derived, always current
            writer.writerow(rec)
    print(f"Exported {len(rows)} records to {out}")
    conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────

def main():
    setup_logging()
    init_db()

    parser = argparse.ArgumentParser(
        description="Welsh Tithe Map Downloader v3",
        formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("discover", help="Catalogue all maps via the /points API")
    p.add_argument("--reset", action="store_true", help="Restart from page 1")
    p.add_argument("--max-pages", type=int, help="Stop after N pages (testing)")

    p = sub.add_parser("metadata", help="Fetch IIIF manifests (title, canvas, size)")
    p.add_argument("--county")
    p.add_argument("--pid", type=int)
    p.add_argument("--limit", type=int)

    p = sub.add_parser("download", help="Download maps + parcels + georeference")
    p.add_argument("--county")
    p.add_argument("--pid", type=int)
    p.add_argument("--pids", help='Comma-separated PIDs or parish names, e.g. "4634773,Llangynllo"')
    p.add_argument("--from-file", help="Text file of targets, one PID or parish name per line")
    p.add_argument("--limit", type=int)
    p.add_argument("--min-parcels", type=int, default=0,
                   help="Skip maps with fewer parcels than this")
    p.add_argument("--include-low", action="store_true",
                   help="Also download maps flagged quality='low'")
    p.add_argument("--image-only", action="store_true",
                   help="Skip the parcels/georeference step")
    p.add_argument("--keep-tiles", action="store_true",
                   help="Keep the raw tile cache after stitching")
    p.add_argument("--warp", action="store_true",
                   help="Also produce a north-up warped GeoTIFF (needs QGIS/GDAL)")
    p.add_argument("--scale", type=int, choices=[1, 2, 4, 8], default=1,
                   help="Downscale factor: 1=full resolution (default), "
                        "2=half, 4=quarter, 8=eighth")

    p = sub.add_parser("list", help="List/search maps in the database")
    p.add_argument("--county")
    p.add_argument("--search", help="Filter by parish or title substring "
                   "(punctuation-insensitive: 'Llanbedr' matches 'Llan-Bedr')")
    p.add_argument("--status", help="Filter by status (discovered/ready/downloaded/...)")
    p.add_argument("--sort", choices=["county", "parcels", "area"], default="county",
                   help="Order by parcel count or covered area (smallest first)")
    p.add_argument("--desc", action="store_true",
                   help="Largest first (with --sort parcels/area)")
    p.add_argument("--limit", type=int, help="Show only the first N rows")
    p.add_argument("--urls", action="store_true",
                   help="Include the online viewer URL for each map")

    p = sub.add_parser(
        "coverage",
        help="Compute covered-area (ha) from cached parcel points for all maps")
    p.add_argument("--force", action="store_true",
                   help="Recompute even for maps that already have a figure")

    p = sub.add_parser("parcels", help="(Re)build parcel GeoJSON files only")
    p.add_argument("--county")
    p.add_argument("--pid", type=int)
    p.add_argument("--refetch", action="store_true")

    p = sub.add_parser("georeference", help="(Re)build GeoJSON + VRT + world file")
    p.add_argument("--county")
    p.add_argument("--pid", type=int)
    p.add_argument("--refetch", action="store_true")
    p.add_argument("--warp", action="store_true",
                   help="Also produce a north-up warped GeoTIFF (needs QGIS/GDAL)")

    p = sub.add_parser("geopackage",
                       help="Bundle all downloaded parcel points into a GeoPackage for QGIS")
    p.add_argument("--county", help="Only include one county")
    p.add_argument("--split", action="store_true",
                   help="One .gpkg per county instead of one file with per-county layers")

    p = sub.add_parser(
        "export-toolkit",
        help="Export sheet(s) for the Cadastral Vectorisation Toolkit "
             "(EPSG:27700 GeoTIFF + seed points)")
    p.add_argument("--toolkit-dir", required=True,
                   help="Path to the toolkit repo (the folder with config.yaml)")
    p.add_argument("--pid", type=int)
    p.add_argument("--county")
    p.add_argument("--sheet-name",
                   help="Override the sheet ID (default: parish name)")
    p.add_argument("--overwrite", action="store_true",
                   help="Re-warp even if the GeoTIFF already exists")

    p = sub.add_parser("quality", help="List maps or set a quality flag")
    p.add_argument("--pid", type=int)
    p.add_argument("--set", choices=["high", "low", "excluded", "none"])

    sub.add_parser("status", help="Progress report")
    sub.add_parser("export", help="Export database to CSV")

    args = parser.parse_args()
    {
        "discover": cmd_discover, "metadata": cmd_metadata,
        "download": cmd_download, "list": cmd_list, "parcels": cmd_parcels,
        "coverage": cmd_coverage,
        "georeference": cmd_georeference, "geopackage": cmd_geopackage,
        "export-toolkit": cmd_export_toolkit, "quality": cmd_quality,
        "status": cmd_status, "export": cmd_export,
    }[args.command](args)


if __name__ == "__main__":
    main()
