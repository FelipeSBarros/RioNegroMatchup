# Aquamatch

Bridging the gap between satellite observations and field data for water quality monitoring.

Aquamatch is a Python package for discovering and downloading Sentinel-2 imagery based on field sampling events, applies atmospheric correction and extracts water quality parameters via ACOLITE, and exports to multiple formats, including data cube and Cloud Optimized GeoTIFF.
Designed for reproducible, automated environmental monitoring workflows — from a single Python call, CLI command, or YAML config.

## Overview

![](./Workflow.png)

**Color coding**: teal for the five pipeline steps, gray/neutral for data artifacts (CSVs, SAFE folders, outputs), amber for the YAML orchestration layer, and purple for the SCL/datacube components.  
**Dashed arrows**: used for two relationships that are optional or indirect: the SCL polygon clip path (only when use_scl=True), and the Step 5 orchestration edges back to Steps 1–4 (since the YAML config drives the others rather than receiving data from them).

---

## Installation

**Requirements:** Python ≥ 3.12

Clone the repository and install dependencies with [Poetry](https://python-poetry.org/):

```bash
git clone https://github.com/FelipeSBarros/aquamatch.git
cd aquamatch
poetry install
```

Or with pip (using the lock file for reproducibility):

```bash
pip install .
```

> `pyyaml` is required for the pipeline config system. It is included in the project dependencies.

---

## Environment Setup

Create a `.env` file in the project root with your API credentials before running any step:  
* `SH` for SentinelHub (see [documentation](https://sentinelhub-py.readthedocs.io/en/latest/configure.html))  
* `DATASPACE` for Copernicus Dataspace (see [documentation](https://documentation.dataspace.copernicus.eu/APIs/S3.html#example-script-to-download-product-using-boto3))   

```env
SH_CLIENT_ID=your_sentinelhub_client_id    # SentinelHub
SH_CLIENT_SECRET=your_sentinelhub_client_secret    # SentinelHub
DATASPACE_ACCESS_KEY=your_copernicus_dataspace_access_key
DATASPACE_SECRET_KEY=your_copernicus_dataspace_secret_key
```

---

### Passing credentials explicitly (e.g. Colab, shared notebooks)

The `.env` file above works well for a persistent local project, but some  environments — Google Colab, CI jobs, notebooks shared between people —  don't have a persistent `.env` file, or need different credentials per run. 
For these cases, credentials can be passed explicitly instead of (or alongside) environment variables.

#### `SentinelCredentials`

```python
from aquamatch.credentials import SentinelCredentials

creds = SentinelCredentials(
    sh_client_id="...",
    sh_client_secret="...",
    dataspace_access_key="...",
    dataspace_secret_key="...",
)
```

Any field left unset defaults to `None`. `SentinelCredentials.from_env()` builds the same object by reading `SH_CLIENT_ID`, `SH_CLIENT_SECRET`, `DATASPACE_ACCESS_KEY`, `DATASPACE_SECRET_KEY` from the environment — this is what happens automatically whenever no explicit credentials are given anywhere in the call chain.

#### Passing credentials to the full pipeline

`run_sentinel_pipeline()` accepts `credentials` as either a `SentinelCredentials` instance or a plain dict:

```python
from aquamatch import run_sentinel_pipeline

run_sentinel_pipeline(
    mode="all",
    credentials={
        "sh_client_id": "...",
        "sh_client_secret": "...",
        "dataspace_access_key": "...",
        "dataspace_secret_key": "...",
    },
)
```

When `credentials` is passed, it's used to build a fresh SentinelHub catalog client, EarthSearch STAC client, and S3 resource for that call — bypassing whatever `.env`/module-level defaults would otherwise apply.
When omitted (the default, `None`), behaviour is unchanged: the module-level clients built once at import time (from `.env`, if present) are used, exactly as before this feature existed.

#### Lower-level functions

The same `credentials` parameter is available on the individual building blocks, for finer control:

```python
from aquamatch.sentinel_data import build_catalog, run_download
from aquamatch.credentials import SentinelCredentials

creds = SentinelCredentials(
    sh_client_id="...",
    sh_client_secret="...",
    dataspace_access_key="...",
    dataspace_secret_key="...",
)

build_catalog(csv_file="...", output_json="...", credentials=creds)
run_download(catalog_json="...", output_dir="...", credentials=creds)
```

`run_download()` also accepts a plain boto3 `s3` resource directly (`run_download(..., s3=my_s3_resource)`), which takes precedence over `credentials` if both are given — useful if you already have an authenticated S3 session managed elsewhere (e.g. an IAM role in a managed notebook environment).

> **Note:** `run_download()` only needs S3 access, so passing
> `credentials` there builds *just* the S3 resource — it never opens a
> connection to the SentinelHub or EarthSearch catalogs. This means
> `run_download(..., credentials=...)` works even in network-restricted
> environments where only the Copernicus Dataspace S3 endpoint is
> reachable.

#### CLI

The same overrides are available from the command line:

```bash
python aquamatch/sentinel_data.py --mode all \
  --sh-client-id "..." \
  --sh-client-secret "..." \
  --dataspace-access-key "..." \
  --dataspace-secret-key "..."
```

Any of the four flags can be given individually — unset ones fall back to `.env`/environment variables rather than being blanked out. So overriding just `--sh-client-id` on the command line while keeping the Dataspace keys in `.env` works as expected.

#### Precedence summary

From highest to lowest priority:

1. An explicit client object passed directly (`s3=...` on `run_download()`, or `catalog=...`/`client=...` on lower-level search functions).
2. `credentials` passed explicitly — as a `SentinelCredentials` instance, a plain dict, or CLI flags (partial CLI flags merge with `.env` for any field not overridden).
3. Environment variables via `.env` — the original, still-default behaviour.

## Step-by-step Workflow

### Step 1 — Prepare in situ data

This package was developed to work with in-situ data from the [OAN](https://www.ambiente.gub.uy/iSIA_OAN/). 
You are therefore expected to save your campaign and station datasets in the './data/original_data' folder.
This step involves reading field campaign data, cleaning measurement values and, if specified, assigning each station its Sentinel-2 tile. Two outputs are produced:

- `campaigns_organized.csv` — full cleaned dataset for analysis
- `campaigns_unique_data.csv` — one row per unique (date, tile) pair, used to drive the satellite search

```python
from aquamatch import run_insitu_pipeline

run_insitu_pipeline(
    stations="data/original_data/my_stations.xlsx",
    campaigns="data/original_data/my_export.xlsx",
)
```

> Pass skip_clean=True if the OAN export was already cleaned before download. [See OAN's documention](https://www.ambiente.gub.uy/iSIA_OAN/guia.html)

**CLI equivalent:**
```bash
python aquamatch/insitu_data.py --mode campaigns \
  --stations your_path/my_stations.xlsx \
  --campaigns your_path/my_export.xlsx
```

---

### Step 2 — Build the satellite catalog

Searches for Sentinel-2 L1C scenes that match each field date and location from `campaigns_unique_data.csv`. Only scenes whose MGRS tile matches the station's assigned tile are kept. For each L1C scene, the corresponding L2A scene is looked up to retrieve the SCL (Scene Classification) asset URL.

```python
from aquamatch import run_sentinel_pipeline

run_sentinel_pipeline(
    csv="data/monitoring_data/campaigns_unique_data.csv",
    time_delta=2,
    cloud_cover=20,
    mode="catalog"
)
```

The result is a `sentinel_catalog.json` file listing matched scenes per field date. Scenes are grouped into three temporal buckets relative to the field date — `same_day`, `previous` (acquired before), and `posterior` (acquired after) — each sorted by `delta_days` then `cloud_cover` ascending so the best candidate is always `bucket[0]`:

```json
[
  {
    "field_date": "2025-11-05",
    "images_found": {
      "same_day": [],
      "previous": [
        {
          "id": "S2B_MSIL1C_20251103T134659_N0511_R024_T21HVD_20251103T170338.SAFE",
          "datetime": "2025-11-03T14:01:38.729Z",
          "cloud_cover": 0.0,
          "href": "s3://eodata/Sentinel-2/MSI/L1C/2025/11/03/S2B_MSIL1C_20251103T134659_N0511_R024_T21HVD_20251103T170338.SAFE/",
          "delta_days": 2,
          "l2a_scl": "https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/21/H/VD/2025/11/S2B_21HVD_20251103_0_L2A/SCL.tif"
        }
      ],
      "posterior": []
    }
  }
]
```

**CLI equivalent:**

```bash
python aquamatch/sentinel_data.py --mode catalog \
  --csv data/monitoring_data/campaigns_unique_data.csv \
  --time-delta 2 \
  --cloud-cover 20
```

---

### Step 3 — Download imagery

Download the SAFE products and SCL assets listed in the catalogue. Any scenes that have already been downloaded are skipped automatically.

The `strategy` parameter controls which scenes are downloaded per field date:

| Strategy | Behaviour |
|----------|-----------|
| `best` (default) | Prefer `same_day`, fall back to `previous`, then `posterior`. Picks up to `max_per_date` scenes. |
| `same_day` | Only download scenes acquired on the exact field date. |
| `previous` | Prefer `same_day`, fall back to `previous` only. Never uses `posterior`. |
| `posterior` | Prefer `same_day`, fall back to `posterior` only. Never uses `previous`. |
| `all` | Download every scene found across all buckets. |

```python
from aquamatch import run_sentinel_pipeline

# Download best matching scene per date (default)
run_sentinel_pipeline(
    strategy="best",
    max_per_date=1,
    download_scl=True,
    mode="download",
)

# Download only same-day scenes, up to 2 per date, cloud cover ≤ 15 %
run_sentinel_pipeline(
    strategy="same_day",
    max_per_date=2,
    max_cloud_cover=15,
    download_scl=True,
    mode="download",
)

# Download everything found
run_sentinel_pipeline(strategy="all", mode="download")
```

> Steps 2 and 3 can also be run together by passing `mode="all"`.

**CLI equivalent:**

```bash
python aquamatch/sentinel_data.py --mode download \
  --strategy best \
  --max-per-date 1 \
  --download-scl
```

---

### Step 4 — Atmospheric correction

Runs [ACOLITE](https://github.com/acolite/acolite) on the downloaded SAFE folders to produce surface reflectance and water quality products (turbidity, SPM, chlorophyll-a, and others) as NetCDF files.

```python
from aquamatch import run_acolite_pipeline

run_acolite_pipeline(
    acolite_executable="/path/to/acolite",
    safe_dir="data/sentinel_downloads/S2A_MSIL1C_20170713T135111_N0500_R024_T21HUD.SAFE",
    output="data/acolite_output",
    limit = [-33.25, -58.45, -33.17, -58.33])
```

A GeoJson's path can also be passed:

```python
run_acolite_pipeline(
    acolite_executable="/path/to/acolite",
    polygon = "/path/polygon.json")
```

For SCL-based water masking, use `with_scl_polygon()` to restrict processing to water pixels only:

```python
run_acolite_pipeline(
    acolite_executable="/path/to/acolite",
    use_scl=True,
    scl_dir="data/sentinel_downloads/scl",
    scl_kwargs={"min_area_m2": 5000},
)
```

To also aggregate the per-scene water masks into a single water polygon GeoPackage datacube after processing — the same output produced by `build_polygon_datacube: true` in the YAML pipeline (see below) — pass `build_polygon_datacube=True`:

```python
result = run_acolite_pipeline(
    acolite_executable="/path/to/acolite",
    safe_dir="data/sentinel_downloads",
    output="data/acolite_output",
    use_scl=True,
    scl_dir="data/sentinel_downloads/scl",
    build_polygon_datacube=True,
    polygon_datacube_path="data/water_polygons.gpkg",
)

result["outputs"]["polygon_datacube"]
# {"status": "ok", "output_path": "data/water_polygons.gpkg", "n_records": 3}
```

`polygon_datacube_path` defaults to `data/water_polygons.gpkg` and `polygon_datacube_overwrite` defaults to `False` (append/skip-duplicates) when omitted — matching `SclSection`'s defaults in the YAML config. The `min_area_m2`/`simplify_tolerance`/`buffer_m` values passed via `scl_kwargs` are reused for the datacube build as well, so there's no separate parameter surface to configure twice.

For batch processing across multiple scenes with per-tile spatial restrictions:

```python
from aquamatch import run_acolite_pipeline
from aquamatch.pipeline_config import TilesSection

# Per-tile restrictions (different polygon or limit per MGRS tile)
tiles = TilesSection.from_dict({
    "21HUD": {"polygon": "data/polygons/21HUD.geojson"},
    "21HVD": {"limit": [-34.2, -56.8, -33.0, -55.1]},
})

result = run_acolite_pipeline(
    acolite_executable="/path/to/acolite",
    safe_dir="data/sentinel_downloads",
    output="data/acolite_output",
    tile_config=tiles,
)
```

> If `tile_config` is omitted, a 0.1° bounding box is derived automatically from `row["latitud"]`/`row["longitud"]`.

**CLI equivalent:**

```bash
python -m aquamatch.acolite_spec \
    --executable /path/to/acolite \
    --safe-dir data/sentinel_downloads/S2A_MSIL1C_20170713T135111_N0500_R024_T21HUD.SAFE \
    --output data/acolite_output
```

> To run acolite with `limit` or `polygon` parameters, used YAML config. [see "Run the full pipeline from a YAML config"](#Run-the-full-pipeline-from-a-YAML-config)

---

## Run the full pipeline from a YAML config

For automated or version-controlled campaigns, the entire workflow can be driven from a single YAML file rather than calling each step individually. This is the recommended approach for production runs and shared projects.

### Generate a template with all parameters at their defaults:

```bash
python -m aquamatch.pipeline_config --generate campaign_2025.yaml
```

### Running from YAML

The generated file includes every parameter at its default value, with inline comments documenting units and valid options. Edit it for your campaign, then run:

```bash
python -m aquamatch.pipeline_config --run campaign_2025.yaml
```

Individual steps can be disabled by setting `enabled: false`:

```yaml
insitu:
  enabled: false   # skip — already prepared

sentinel:
  enabled: true
  time_delta: 2
  cloud_cover: 20

download:
  enabled: true
  # Download selection strategy. One of: best | same_day | previous | posterior | all
  # best: prefer same_day, fall back to previous then posterior (default)
  strategy: best
  # Maximum scenes to download per field date (ignored for strategy: all)
  max_per_date: 1
  # Optional secondary cloud cover ceiling applied at download time
  max_cloud_cover: null
  download_scl: true

acolite:
  enabled: true
  acolite_executable: /path/to/acolite/acolite.py
  scl:
    use_scl: true
    min_area_m2: 5000

tiles:
  21HUD:
    polygon: data/polygons/21HUD.geojson
  21HVD:
    limit: [-34.2, -56.8, -33.0, -55.1]
```

### Dry-run 

Validate config and log steps without executing:

```bash
python -m aquamatch.pipeline_config --run campaign_2025.yaml --dry-run
```

### Force process

Force reprocess — ignore existing outputs and reprocess all scenes:

```bash
python -m aquamatch.pipeline_config --run campaign_2025.yaml --force
```

### Per-tile spatial restrictions

The `tiles`: section assigns a spatial restriction to each Sentinel-2 MGRS tile. The same boundary is then applied consistently across every scene processed for that tile. Restrictions are resolved in this precedence order:

1. **Static polygon** from `tiles:` — highest priority. If a tile has a polygon configured, it is applied directly to ACOLITE and SCL-based clipping (`use_scl`) is suppressed for that tile, since the static polygon already defines the water boundary precisely.
2. **SCL-derived polygon** (`use_scl: true`) — used when the tile has no static polygon. A water mask is extracted from the SCL asset and applied as the processing boundary.
3. **Static limit** from `tiles:` — applied when no polygon is available from either source above.
4. **No restriction** — full scene is processed when the tile is not listed in `tiles:` and SCL clipping is disabled or unavailable.

```yaml
tiles:
  21HUD:
    polygon: data/polygons/21HUD.geojson   # hand-drawn or pre-processed boundary
  21HVD:
    limit: [-34.2, -56.8, -33.0, -55.1]   # bounding box [S, W, N, E]
  21HWD:
    # no entry — full scene processed
```

> The tile ID is extracted automatically from the SAFE folder filename (e.g. `T21HUD` in `S2A_MSIL1C_20250801T101031_N0500_R024_T21HUD_...SAFE`), so no manual mapping between files and tiles is needed.

> The same precedence logic applies when using run_batch() programmatically — see Step 4 above.

---

## Utilities

### Temporal opportunity cost analysis

Before committing to a temporal tolerance for your campaign, it is useful to understand how many field dates will have a valid Sentinel-2 acquisition available and what cloud cover to expect at each tolerance level. `analyze_temporal_opportunity` quantifies this directly from the catalog produced by Step 2.

For every tolerance `d` from 0 to `max_delta_days` the function:

1. Determines whether at least one image exists with `delta_days <= d` for each field date.
2. Computes **availability** (% of field dates with a valid image) and **opportunity cost** (`1 - availability`).
3. Selects the best image per date (minimum `delta_days`, then minimum `cloud_cover`) and reports mean and median cloud cover of the selected set.

A publication-quality three-panel PNG figure is saved to `output_figure`:

| Panel | Content |
|-------|---------|
| Left | Availability (%) vs temporal tolerance (days) |
| Centre | Opportunity cost (%) vs temporal tolerance (days) |
| Right | Mean and median cloud cover (%) vs temporal tolerance (days) |

```python
from aquamatch.utils import analyze_temporal_opportunity

df = analyze_temporal_opportunity(
    catalog_json="data/sentinel_downloads/sentinel_catalog.json",
    output_figure="reports/temporal_opportunity.png",
    max_delta_days=7,
)

print(df[["delta_days", "availability", "opportunity_cost", "mean_cloud_cover"]])
```

```
   delta_days  availability  opportunity_cost  mean_cloud_cover
0           0          42.9              57.1               3.2
1           1          71.4              28.6               4.8
2           2          85.7              14.3               5.1
3           3          92.9               7.1               6.3
4           4          92.9               7.1               6.3
5           5         100.0               0.0               7.0
6           6         100.0               0.0               7.0
7           7         100.0               0.0               7.0
```

The returned DataFrame contains one row per tolerance value with columns `delta_days`, `n_dates`, `n_available`, `availability`, `opportunity_cost`, `mean_cloud_cover`, and `median_cloud_cover`. Pass `return_dataframe=False` if only the figure is needed.

**What to consider when choosing a tolerance**
The table frames a trade-off between temporal fidelity and spatial coverage:

A strict same-day requirement (d=0) gives the most scientifically defensible matchups but leaves 57% of your field dates unmatched — a significant loss of usable data.
d=1 or d=2 is usually the pragmatic sweet spot for water quality remote sensing, where surface conditions can change meaningfully over days but same-day coverage is rare.
If your target variable changes slowly (e.g. long-term turbidity trends in a large reservoir), d=3 or d=5 may be acceptable and recovers nearly all dates.
If your variable is highly dynamic (e.g. algal bloom events, flood pulses), even d=1 may introduce unacceptable temporal mismatch and you should be cautious about any row beyond d=0.

The cloud cover column is a sanity check — if mean cloud cover were climbing sharply with tolerance (say, above 20–30%), it would signal that the additional scenes being recovered are systematically cloudy and may not produce usable retrievals even after atmospheric correction.

### L2W pixel-value extraction at campaign stations

Once you have an ACOLITE L2W product and a table of field-campaign station coordinates, `extract_l2w_pixel_values` extracts the corresponding pixel value at each station — the mean of a small window (default 3x3) centered on the pixel nearest the station's coordinates. This is the first step toward comparing satellite-derived values against in-situ measurements: it appends new columns to your existing station table rather than returning a fresh one, so identifying columns (station id, date, tile, etc.) survive for a later join against measured values.

Stations whose coordinates fall outside the scene entirely (e.g. the wrong Sentinel-2 tile) are flagged rather than given a misleading nearest-edge value — `in_bounds=False` distinguishes "wrong tile" from "in bounds but every pixel in the window is cloud-masked" (both give `NaN` for the mean, only the former also gives `in_bounds=False`).

```python
from aquamatch.utils import extract_l2w_pixel_values
import pandas as pd

stations = pd.read_csv("data/monitoring_data/campaigns_unique_data.csv")

result = extract_l2w_pixel_values(
    l2w_nc="data/acolite_output/.../S2B_MSI_2024_07_08_13_51_48_T21HVD_L2W.nc",
    stations=stations,
    variables=["chl_oc3"],
)

print(result[["date", "s2_tile", "chl_oc3_mean", "chl_oc3_n_valid_px", "in_bounds"]])
```

```
         date s2_tile  chl_oc3_mean  chl_oc3_n_valid_px  in_bounds
0  2025-11-04   21HVD      9.082612                    9       True
1  2025-08-13   21HWD           NaN                    0      False
2  2020-02-05   21HVD           NaN                    0       True
```

(Row 0 is in the scene and mostly cloud-free — 9/9 valid pixels. Row 1 is on a different tile than this scene, correctly flagged out of bounds. Row 2 is on the right tile but lands on a masked pixel — `in_bounds=True` with `n_valid_px=0` shows it's cloud/land masking, not a tile mismatch.)

For each requested variable `var`, the returned DataFrame gains:

| Column | Type | Description |
|--------|------|--------------|
| `{var}_mean` | float | NaN-aware mean of the pixel window, or `NaN` if out of bounds or fully masked |
| `{var}_n_valid_px` | int | Non-NaN pixels contributing to the mean (out of up to `window_size**2`) — useful for judging extraction quality |
| `in_bounds` | bool | Whether the station falls within the scene's footprint at all (shared across all variables) |

By default `variables=None` extracts every real data variable in the file, `window_size` must be a positive odd integer, and `lat_col`/`lon_col` default to `"latitud"`/`"longitud"` to match this project's campaign CSVs — pass your own column names if your station table differs. This works on one L2W scene at a time — for a table of stations spanning many campaign dates, use `extract_datacube_pixel_values` below instead, which matches each row to its own date automatically.

### L2W pixel-value extraction from a datacube (multi-date)

`extract_l2w_pixel_values` above is scoped to one L2W scene, so calling it once with a single file against a station table spanning many dates gives every row the same scene's value — not what you want for a real matchup analysis. `extract_datacube_pixel_values` is the multi-date counterpart: it reads a Zarr datacube built by `append_l2w_to_datacube` (Step 4's datacube option) and, for every row of `stations`, selects the pixel window at *that row's own* matched date — never a shared or fixed date.

```python
from aquamatch.utils import extract_datacube_pixel_values
import pandas as pd

stations = pd.read_csv("data/monitoring_data/campaigns_unique_data.csv")

result = extract_datacube_pixel_values(
    datacube_path="data/data_cube/l2w_datacube.zarr",
    stations=stations,
    variables=["chl_oc3"],
    time_tolerance_days=5,
)

print(result[["date", "chl_oc3_mean", "chl_oc3_n_valid_px", "in_bounds", "matched_time_delta_days"]])
```

Rows sharing the same matched date reuse a single load of that date's 2-D slice, so this is also more efficient than reopening one L2W file per station.

| Column | Type | Description |
|--------|------|--------------|
| `{var}_mean` | float | NaN-aware mean of the pixel window at the row's matched date, or `NaN` if unmatched/out of bounds/fully masked |
| `{var}_n_valid_px` | int | Non-NaN pixels contributing to the mean |
| `in_bounds` | bool | `True` only when both spatially inside the cube's grid *and* a datacube date was found within `time_tolerance_days` |
| `matched_time_delta_days` | float | Days between the row's date and the matched datacube date (`NaN` when unmatched) — a diagnostic for spotting silent date-matching mismatches |

`time_tolerance_days` (default `0`) controls how close a datacube date must be to a station's date to count as a match — widen it if your campaign dates don't line up exactly with the scenes you processed into the cube. `datacube_crs` defaults to `"EPSG:4326"` (matching `append_l2w_to_datacube`'s own default); pass the actual `target_crs` you built the cube with if it differs.

### Satellite-vs-in-situ scatter plot & correlation analysis

Once you have satellite pixel values, `plot_satellite_vs_insitu` joins them against your in-situ measurements and produces a validation scatter plot with correlation statistics — this is the workflow the two functions above feed into. It filters a long/tidy campaigns CSV (e.g. `data/monitoring_data/campaigns_organized.csv`, one row per measured parameter per sample) to a single parameter, calls `extract_datacube_pixel_values` internally, and plots in-situ (x-axis, reference) against satellite (y-axis, retrieved) values with a 1:1 line.

```python
from aquamatch.utils import plot_satellite_vs_insitu

matched, stats = plot_satellite_vs_insitu(
    campaigns_csv="data/monitoring_data/campaigns_organized.csv",
    datacube_path="data/data_cube/l2w_datacube.zarr",
    output_figure="reports/satellite_vs_insitu_chl.png",
    time_tolerance_days=5,
)

print(stats)  # {"n": ..., "r": ..., "r2": ..., "rmse": ..., "bias": ...}
```

The default column names (`parametro_col="parametro"`, `value_col="organized_value"`, `insitu_parametro="Clorofila_a_(lab)"`) match this project's own cleaned campaigns CSV — if your in-situ table uses different column names (e.g. a `valor_transformado` column instead of `organized_value`), pass them explicitly:

```python
matched, stats = plot_satellite_vs_insitu(
    campaigns_csv="data/monitoring_data/campaigns_organized.csv",
    datacube_path="data/data_cube/l2w_datacube.zarr",
    output_figure="reports/satellite_vs_insitu_chl.png",
    value_col="valor_transformado",
    time_tolerance_days=5,
)
```

`stats` has keys `n`, `r` (Pearson correlation), `r2`, `rmse`, and `bias` (`satellite - insitu`), computed with plain numpy — no scipy/scikit-learn dependency. Zero matchups (e.g. `time_tolerance_days` too strict, or wrong tile) is not an error: `stats["n"] == 0`, all other stats are `NaN`, and the figure is still written with a "No matchups found" annotation instead of crashing — check the logged warning to see why. In-situ chlorophyll-a is conventionally reported in µg/L, numerically identical to `chl_oc3`'s mg/m³, so no unit conversion is applied for that default pairing.

### Download completeness audit

Before atmospheric correction, it's useful to know exactly which cataloged scenes are actually on disk. `audit_downloads` compares the catalog from Step 2 against a download root and reports SAFE/SCL presence per scene — one row per image across all buckets (`same_day`, `previous`, `posterior`).

```python
from aquamatch.utils import audit_downloads

df = audit_downloads(
    catalog_json="data/sentinel_downloads/sentinel_catalog.json",
    output_dir="data/sentinel_downloads",
)

print(df.head(4))
print(df["bucket"].value_counts())
print(f"{df['all_downloaded'].sum()}/{len(df)} scenes fully downloaded")
```

```
   field_date                                                          scene_id  delta_days  cloud_cover  safe_exists  scl_exists  all_downloaded    bucket
0  2025-11-05  S2B_MSIL1C_20251103T134659_N0511_R024_T21HVD_20251103T170338.SAFE           2         0.00        False       False           False  previous
1  2025-11-05  S2B_MSIL1C_20251103T134659_N0511_R024_T21HUD_20251103T170338.SAFE           2         0.00        False       False           False  previous
2  2025-11-05  S2A_MSIL1C_20251031T135131_N0511_R024_T21HUD_20251031T152617.SAFE           5         6.77        False       False           False  previous
3  2025-11-05  S2A_MSIL1C_20251031T135131_N0511_R024_T21HVD_20251031T152617.SAFE           5        35.71        False       False           False  previous

bucket
previous     136
posterior    126
same_day      28
Name: count, dtype: int64

0/290 scenes fully downloaded
```

The returned DataFrame has one row per cataloged image:

| Column | Type | Description |
|--------|------|--------------|
| `field_date` | str | Field sampling date |
| `scene_id` | str | Sentinel-2 scene identifier |
| `delta_days` | int | Days between the scene acquisition and the field date |
| `cloud_cover` | float | Scene cloud cover (%) |
| `safe_exists` | bool | Whether the SAFE folder is present under `output_dir` |
| `scl_exists` | bool or None | Whether the SCL GeoTIFF is present (`None` when `download_scl=False`) |
| `all_downloaded` | bool | Whether everything required for this scene is already on disk |
| `bucket` | str or None | `same_day`, `previous`, or `posterior` (`None` for legacy flat-list catalogs with no bucket info) |

Set `download_scl=False` if your workflow doesn't need SCL files — `scl_exists` and the `all_downloaded` check then depend only on the SAFE folder. Filter `df[~df["all_downloaded"]]` to get exactly the scenes still missing before deciding what to (re)download.