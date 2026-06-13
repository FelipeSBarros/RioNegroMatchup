# Aquamatch

Python package and scripts to match Sentinel-2 satellite imagery with in situ water quality field measurements, apply atmospheric correction, and validate remote sensing water quality products.

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

```env
SH_CLIENT_ID=your_sentinelhub_client_id
SH_CLIENT_SECRET=your_sentinelhub_client_secret
DATASPACE_ACCESS_KEY=your_copernicus_dataspace_access_key
DATASPACE_SECRET_KEY=your_copernicus_dataspace_secret_key
```

See the [Copernicus Dataspace documentation](https://documentation.dataspace.copernicus.eu/APIs/S3.html#example-script-to-download-product-using-boto3) for details on obtaining your access key and secret.

---

## Step-by-step Workflow

### Step 1 — Prepare in situ data

Reads field campaign data from the [OAN](https://www.ambiente.gub.uy/iSIA_OAN/), cleans measurement values, assigns each station its Sentinel-2 tile, and produces two outputs:

- `campaigns_organized.csv` — full cleaned dataset for analysis
- `campaigns_unique_data.csv` — one row per unique (date, tile) pair, used to drive the satellite search

```bash
python aquamatch/insitu_data.py --mode campaigns
```

To use files in non-default locations:

```bash
python aquamatch/insitu_data.py --mode campaigns \
  --stations data/original_data/my_stations.xlsx \
  --campaigns data/original_data/my_export.xlsx
```

> The `--skip-clean` flag is available if the OAN export has already been cleaned before download. [See OAN's documention](https://www.ambiente.gub.uy/iSIA_OAN/guia.html)

---

### Step 2 — Build the satellite catalog

Searches for Sentinel-2 L1C scenes that match each field date and location from `campaigns_unique_data.csv`. Only scenes whose MGRS tile matches the station's assigned tile are kept. For each L1C scene, the corresponding L2A scene is looked up to retrieve the SCL (Scene Classification) asset URL.

The result is a `sentinel_catalog.json` file listing matched scenes per field date.

```bash
python aquamatch/sentinel_data.py --mode catalog \
  --csv data/monitoring_data/campaigns_unique_data.csv \
  --time-delta 2 \
  --cloud-cover 20
```

---

### Step 3 — Download imagery

Downloads the SAFE products and SCL assets listed in the catalog. Already-downloaded scenes are skipped automatically.

```bash
python aquamatch/sentinel_data.py --mode download \
  --download-scl
```

> You can run both steps (build catalog and download images) using `--mode all`

---

### Step 4 — Atmospheric correction

Runs [ACOLITE](https://github.com/acolite/acolite) on the downloaded SAFE folders to produce surface reflectance and water quality products (turbidity, SPM, chlorophyll-a, and others) as NetCDF files.

```python
from aquamatch.acolite_spec import AcoliteConfig, IOConfig

cfg = AcoliteConfig(
    acolite_executable="/path/to/acolite",
    io=IOConfig(
        inputfile="data/sentinel_downloads/S2A_MSIL1C_20170713T135111_N0500_R024_T21HUD.SAFE",
        output="data/acolite_output",
        limit=(-33.25, -58.45, -33.17, -58.33),  # S, W, N, E
    ),
)

result = cfg.run()
```

For SCL-based water masking, use `with_scl_polygon()` to restrict processing to water pixels only:

```python
result = cfg.with_scl_polygon(
    "data/sentinel_downloads/scl/S2B_MSIL1C_20200513T135109_N0500_R024_T21HVD_20230430T050652_SCL.tif"
).run()
```

---

## Run the full pipeline from a YAML config

The pipeline can also be driven entirely from a single YAML file — one file per campaign, version-controlled alongside your data.

**Generate a template:**

```bash
python -m aquamatch.pipeline_config --generate campaign_2025.yaml
```

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
  time_delta_days: 2
  cloud_cover_max: 20

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

**Dry-run** (validate config and log steps without executing):

```bash
python -m aquamatch.pipeline_config --run campaign_2025.yaml --dry-run
```

**Force reprocess** (ignore existing outputs and reprocess all scenes):

```bash
python -m aquamatch.pipeline_config --run campaign_2025.yaml --force
```

### Per-tile spatial restrictions

The `tiles:` section of the config lets you define a spatial restriction for each Sentinel-2 MGRS tile, so the same boundary is applied consistently across every scene processed for that tile — no need to specify it on each run.

For each tile, set either `polygon` (a GeoJSON or WKT file path) or `limit` (a `[south, west, north, east]` bounding box in decimal degrees), or omit the tile entirely to process the full scene.

```yaml
tiles:
  21HUD:
    polygon: data/polygons/21HUD.geojson   # hand-drawn or pre-processed boundary
  21HVD:
    limit: [-34.2, -56.8, -33.0, -55.1]   # bounding box [S, W, N, E]
  21HWD:
    # no entry — full scene processed
```

The restriction is resolved per scene during atmospheric correction following this precedence order:

1. **Static polygon** from `tiles:` — highest priority. If a tile has a polygon configured, it is applied directly to ACOLITE and SCL-based clipping (`use_scl`) is suppressed for that tile, since the static polygon already defines the water boundary precisely.
2. **SCL-derived polygon** (`use_scl: true`) — used when the tile has no static polygon. A water mask is extracted from the SCL asset and applied as the processing boundary.
3. **Static limit** from `tiles:` — applied when no polygon is available from either source above.
4. **No restriction** — full scene is processed when the tile is not listed in `tiles:` and SCL clipping is disabled or unavailable.

> The tile ID is extracted automatically from the SAFE folder filename (e.g. `T21HUD` in `S2A_MSIL1C_20250801T101031_N0500_R024_T21HUD_...SAFE`), so no manual mapping between files and tiles is needed.

---

## Programmatic usage

For scripting and integration into custom workflows, all pipeline steps can be called directly without a config file.

```bash
# Step 1 — prepare in situ data
python aquamatch/insitu_data.py --mode campaigns

# Step 2 — build catalog (±2 days, max 20% cloud cover)
python aquamatch/sentinel_data.py --mode catalog \
  --csv data/monitoring_data/campaigns_unique_data.csv \
  --time-delta 2 \
  --cloud-cover 20

# Step 3 — download imagery and SCL assets
python rionegromatchup/sentinel_data.py --mode download \
  --download-scl
```

```python
from pathlib import Path
from aquamatch.acolite_spec import AcoliteConfig, IOConfig
from aquamatch.pipeline_config import TilesSection, TileEntry

cfg = AcoliteConfig(
    acolite_executable="/path/to/acolite",
    io=IOConfig(inputfile="", output=""),
)

safe_list = sorted(Path("data/sentinel_downloads").glob("*.SAFE"))
scl_dir = Path("data/sentinel_downloads/scl")

# Define per-tile spatial restrictions
tiles = TilesSection.from_dict({
    "21HUD": {"polygon": "data/polygons/21HUD.geojson"},
    "21HVD": {"limit": [-34.2, -56.8, -33.0, -55.1]},
})

results = cfg.run_batch(
    safe_list=safe_list,
    base_output="data/acolite_output",
    use_scl=True,
    scl_dir=scl_dir,
    scl_kwargs={"min_area_m2": 5000},
    tile_config=tiles,
    continue_on_error=True,
    skip_existing=True,    # set False to reprocess all scenes
)
```

You can also resolve tile restrictions per row when building configs from campaign data:

```python
from aquamatch.acolite_spec import AcoliteConfig
from aquamatch.pipeline_config import TilesSection

tiles = TilesSection.from_dict({
    "21HUD": {"polygon": "data/polygons/21HUD.geojson"},
    "21HVD": {"limit": [-34.2, -56.8, -33.0, -55.1]},
})

# row is a pandas Series from campaigns_unique_data.csv
cfg = AcoliteConfig.from_campaigns_row(
    row=row,
    acolite_executable="/path/to/acolite",
    base_output="data/acolite_output",
    inputfile=str(safe_path),
    tile_config=tiles,
)
```

The spatial restriction is resolved automatically from `row["s2_tile"]`. If `tile_config` is omitted, the original behaviour applies: a 0.1° bounding box is derived from the row's `latitud`/`longitud` coordinates.
