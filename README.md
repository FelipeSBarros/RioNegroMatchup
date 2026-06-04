# Río Negro Matchup

Python package and scripts to match Sentinel-2 satellite imagery with in situ water quality field measurements, apply atmospheric correction, and validate remote sensing water quality products.

## Overview

![](./Workflow.png)

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
python rionegromatchup/insitu_data.py --mode campaigns
```

To use files in non-default locations:

```bash
python rionegromatchup/insitu_data.py --mode campaigns \
  --stations data/original_data/my_stations.xlsx \
  --campaigns data/original_data/my_export.xlsx
```

> The `--skip-clean` flag is available if the OAN export has already been cleaned before download. [See OAN's documention](https://www.ambiente.gub.uy/iSIA_OAN/guia.html)

---

### Step 2 — Build the satellite catalog

Searches for Sentinel-2 L1C scenes that match each field date and location from `campaigns_unique_data.csv`. Only scenes whose MGRS tile matches the station's assigned tile are kept. For each L1C scene, the corresponding L2A scene is looked up to retrieve the SCL (Scene Classification) asset URL.

The result is a `sentinel_catalog.json` file listing matched scenes per field date.

```bash
python rionegromatchup/sentinel_data.py --mode catalog \
  --csv data/monitoring_data/campaigns_unique_data.csv \
  --time-delta 2 \
  --cloud-cover 20
```

---

### Step 3 — Download imagery

Downloads the SAFE products and SCL assets listed in the catalog. Already-downloaded scenes are skipped automatically.

```bash
python rionegromatchup/sentinel_data.py --mode download \
  --download-scl
```

> You can run both steps (build catalog and download images) using `--mode all`

---

### Step 4 — Atmospheric correction

Runs [ACOLITE](https://github.com/acolite/acolite) on the downloaded SAFE folders to produce surface reflectance and water quality products (turbidity, SPM, chlorophyll-a, and others) as NetCDF files.

```python
from rionegromatchup.acolite_spec import AcoliteConfig, IOConfig

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
    "data/sentinel_downloads/scl/S2A_MSIL1C_20170713T135111_N0500_R024_T21HUD_SCL.tif"
).run()
```

---

## Batch Processing

```bash
# Step 1 — prepare in situ data
python rionegromatchup/insitu_data.py --mode campaigns

# Step 2 — build catalog (±2 days, max 20% cloud cover)
python rionegromatchup/sentinel_data.py --mode catalog \
  --csv data/monitoring_data/campaigns_unique_data.csv \
  --time-delta 2 \
  --cloud-cover 20

# Step 3 — download imagery and SCL assets
python rionegromatchup/sentinel_data.py --mode download \
  --download-scl
```

```python
from pathlib import Path
from rionegromatchup.acolite_spec import AcoliteConfig, IOConfig

cfg = AcoliteConfig(
    acolite_executable="/path/to/acolite",
    io=IOConfig(inputfile="", output=""),
)

safe_list = sorted(Path("data/sentinel_downloads").glob("*.SAFE"))
scl_dir = Path("data/sentinel_downloads/scl")

results = cfg.run_batch(
    safe_list=safe_list,
    base_output="data/acolite_output",
    use_scl=True,
    scl_dir=scl_dir,
    scl_kwargs={"min_area_m2": 5000},
    continue_on_error=True,
)
```
