# Aquamatch Workflow

Source diagram for the "Overview" section of [`README.md`](./README.md). Edit this file (and copy the same Mermaid block into the README) whenever a step, artifact, or utility changes — no image regeneration needed.

```mermaid
flowchart TD
    OAN["OAN xlsx files"]:::io
    STEP1["<b>Step 1 — In-situ data</b><br/>run_insitu_pipeline()<br/><i>clean OAN campaigns, assign S2 tile</i>"]:::step
    ORG["campaigns_organized.csv"]:::artifact
    UNIQ["campaigns_unique_data.csv"]:::artifact

    OAN --> STEP1
    STEP1 --> ORG
    STEP1 --> UNIQ

    SHAPI["SentinelHub API"]:::io
    ESTAC["EarthSearch STAC"]:::io
    STEP2["<b>Step 2 — Satellite catalog</b><br/>run_sentinel_pipeline(mode='catalog')<br/><i>match field dates to S2 scenes</i>"]:::step
    CATALOG["sentinel_catalog.json"]:::artifact
    OPPCOST["analyze_temporal_opportunity()<br/><i>availability vs. tolerance report</i>"]:::utility

    UNIQ --> STEP2
    SHAPI --> STEP2
    ESTAC --> STEP2
    STEP2 --> CATALOG
    CATALOG -.-> OPPCOST

    DATASPACE["Copernicus Dataspace"]:::io
    STEP3["<b>Step 3 — Download imagery</b><br/>run_sentinel_pipeline(mode='download')<br/><i>SAFE products + SCL assets via S3</i>"]:::step
    SAFE["*.SAFE folders"]:::artifact
    SCLTIF["scl/*.tif"]:::artifact
    AUDIT["audit_downloads()<br/><i>SAFE/SCL completeness report</i>"]:::utility

    CATALOG --> STEP3
    DATASPACE --> STEP3
    STEP3 --> SAFE
    STEP3 --> SCLTIF
    SAFE -.-> AUDIT
    SCLTIF -.-> AUDIT

    SCLMASK["SCL water masking<br/>with_scl_polygon()<br/><i>extract water polygon (use_scl=True)</i>"]:::scl
    SCLTIF -.-> SCLMASK

    STEP4["<b>Step 4 — Atmospheric correction</b><br/>run_acolite_pipeline()<br/><i>ACOLITE: DSF, glint correction</i>"]:::step
    SAFE --> STEP4
    SCLMASK -.->|clip| STEP4

    L2W["L2W .nc products<br/>turbidity, SPM, chl-a, FAI, NDWI…"]:::product
    DCUBE["Datacube<br/>Zarr / COG"]:::product
    POLY["water_polygons.gpkg"]:::product

    STEP4 --> L2W
    STEP4 --> DCUBE
    SCLMASK -.-> POLY

    L2WEX["extract_l2w_pixel_values()<br/><i>single-scene station values</i>"]:::utility
    DCEX["extract_datacube_pixel_values()<br/><i>multi-date station values</i>"]:::utility
    SCATTER["plot_satellite_vs_insitu()<br/><i>scatter + r / r² / RMSE / bias</i>"]:::utility

    L2W --> L2WEX
    DCUBE --> DCEX
    ORG -.-> SCATTER
    DCEX --> SCATTER

    subgraph ORCH["optional orchestration"]
        direction LR
        GEN["--generate<br/><i>write YAML template</i>"]:::config
        STEP5["<b>Step 5 — Pipeline config</b><br/><i>YAML-driven, one file per campaign</i>"]:::config
        RUN["--run<br/><i>execute Steps 1–4</i>"]:::config
        GEN --> STEP5 --> RUN
    end

    STEP5 -.orchestrates.-> STEP1
    STEP5 -.orchestrates.-> STEP2
    STEP5 -.orchestrates.-> STEP3
    STEP5 -.orchestrates.-> STEP4

    classDef step fill:#0f766e,stroke:#0b5a54,color:#ffffff,font-weight:bold
    classDef artifact fill:#57606f,stroke:#3d4451,color:#ffffff
    classDef io fill:#57606f,stroke:#3d4451,color:#ffffff
    classDef product fill:#4338ca,stroke:#33269a,color:#ffffff
    classDef scl fill:#4338ca,stroke:#33269a,color:#ffffff
    classDef config fill:#92400e,stroke:#733309,color:#ffffff
    classDef utility fill:#1d4ed8,stroke:#1638a8,color:#ffffff
```

**Color coding**: teal for the five pipeline steps, gray/neutral for data artifacts and external inputs, purple for the SCL/datacube/water-quality-product components, amber for the YAML orchestration layer, and blue for the analysis/validation utilities (`analyze_temporal_opportunity`, `audit_downloads`, `extract_l2w_pixel_values`, `extract_datacube_pixel_values`, `plot_satellite_vs_insitu`).
**Dashed arrows**: mark optional or indirect relationships — a pipeline output feeding one of the utilities above, the SCL polygon clip path (only when `use_scl=True`), and the Step 5 orchestration edges back to Steps 1–4 (since the YAML config drives the others rather than receiving data from them).
