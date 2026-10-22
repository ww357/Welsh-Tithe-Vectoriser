# Step 00 — Download (Welsh Tithe Map Downloader)

The Welsh toolkit's inputs — the georeferenced map raster and the apportionment
seed points — are produced by the **Welsh Tithe Map Downloader**, which is
vendored into this folder and driven through `run.py` like any other step.

## Files in this folder

- `tithe_downloader.py` — the downloader (copied from the `Welsh-Tithes-Downloader`
  repo). Its catalogue database and downloaded scans live in
  `steps/00_download/tithe_maps/` (gitignored).
- `download.py` — a thin wrapper `run.py` calls. It forwards the downloader's
  subcommands and, for `export-toolkit`, fills in `--toolkit-dir` with this
  toolkit's root automatically.

To update the downloader later, re-copy `tithe_downloader.py` from its repo.

## Environment

No separate environment. The downloader runs in **maptools**, which already has
`requests`, `pillow`, `tqdm`, `numpy` and **GDAL** — so `export-toolkit` finds
`gdalwarp` / `gdaltransform` from maptools itself (no QGIS needed).

## Running step 00

```bash
conda activate maptools

# one-off: catalogue every NLW tithe map (~1,074 maps; resumable)
python run.py download discover

# download one or more maps (scan + parcel points + georeferencing)
python run.py download fetch --county Anglesey        # 'fetch' == the downloader's 'download'
                                                      # or --pids "Llangynllo,4634773"

# export toolkit-ready sheets INTO data/raw and data/parcel_points
python run.py download export-toolkit --county Anglesey
```

`run.py download <subcommand>` forwards to `tithe_downloader.py <subcommand>`;
`fetch` and `maps` are aliases for the downloader's `download` and `list` so you
never have to type `download download`. Any subcommand the wrapper does not list
still works — run `python steps/00_download/tithe_downloader.py --help`.

## What it writes into the toolkit

Per sheet:

- `data/raw/<sheet>/<sheet>.tif` — north-up **EPSG:27700** GeoTIFF at **0.5 m/px**,
  exactly what `run.py patchify` expects.
- `data/parcel_points/<sheet>_points.gpkg` — one seed point per apportionment
  parcel, EPSG:27700, matched to the raster. The per-sheet name (`<sheet>_points`)
  is auto-detected by the parcel steps — no per-sheet config change.

The seed-point layer carries `rowid` (1..N, the join key the parcel vectorise step
uses), `Easting`/`Northing`, the apportionment attributes (`ParishName, ParcelID,
Landowner, Occupier, FieldName_Desc, AreaName, CultivationState`), the imperial
area as separate `Acres` / `Rods` / `Perches` columns, and a computed
**`area_hectares`** total, plus `rent_decimal_pounds`, `pixel_x`/`pixel_y`,
`map_pid`, `nlw_id`.

> **Size-cap note:** the parcel size cap must use `area_hectares` (the true total),
> not the `Acres` column (whole-acres only — it ignores roods and perches). This is
> pinned in `config.yaml`: `parcels.size_column: area_hectares`, `parcels.size_unit: ha`.
> Leave those set for Welsh sheets.

## Then continue in the toolkit

```bash
python run.py patchify --sheet <sheet> --mask
python run.py status --sheet <sheet>          # check progress at any point
# ... the rest of the pipeline as normal
```
