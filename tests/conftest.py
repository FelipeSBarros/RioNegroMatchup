from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from aquamatch.acolite_spec import AcoliteConfig, IOConfig

# Prevent the module-level Client.open() network call in sentinel_data.py
# from firing during test collection.
_pystac_patch = patch(
    "pystac_client.Client.open",
    return_value=MagicMock(),
)
_pystac_patch.start()


# ---------------------------------------------------------------------------
# Shared test helpers
# ---------------------------------------------------------------------------

# Canonical SAFE name — tile 21HUD
_SAFE_NAME = "S2A_MSIL1C_20250801T101031_N0500_R024_T21HUD_20230919T094731.SAFE"


def _write_yaml(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(content)
    return p


def _make_safe(tmp_path, name=_SAFE_NAME):
    safe = tmp_path / name
    safe.mkdir(parents=True, exist_ok=True)
    (safe / "dummy.xml").write_text("<root/>")
    return safe


def _make_cfg(tmp_path):
    exe = tmp_path / "acolite"
    exe.write_text("#!/bin/sh")
    exe.chmod(0o755)
    return AcoliteConfig(
        acolite_executable=str(exe),
        io=IOConfig(inputfile="", output=str(tmp_path)),
    )


_SCENE_ID = "S2A_MSIL1C_20240315T135111_N0500_R024_T21HUD_20240315T160000"


def _make_catalog(tmp_path: Path) -> Path:
    catalog_data = [
        {
            "field_date": "2024-03-15",
            "images_found": {
                "same_day": [
                    {
                        "id": _SCENE_ID,
                        "href": f"https://eodata.dataspace.copernicus.eu/eodata/{_SCENE_ID}/path",
                        "l2a_scl": None,
                        "delta_days": 0,
                        "cloud_cover": 5,
                        "datetime": "2024-03-15T13:51:11Z",
                    }
                ],
                "previous": [],
                "posterior": [],
            },
        }
    ]
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps(catalog_data))
    return path


def _fake_s3_with_bucket():
    fake_bucket = MagicMock(name="fake_bucket")
    fake_s3 = MagicMock(name="fake_s3")
    fake_s3.Bucket.return_value = fake_bucket
    return fake_s3, fake_bucket
