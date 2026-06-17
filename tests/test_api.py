"""
Tests for aquamatch/api.py.

api.py is a pure re-export module — it contains no logic of its own.
The tests here verify:
  1. All three wrappers are importable from aquamatch.api.
  2. Each name resolves to the correct function in its home module
     (identity check — not a copy).
  3. __all__ is defined and contains exactly the three public names.
  4. Calling a wrapper via the api import produces the same result as
     calling it via its home module import (end-to-end smoke test for
     run_insitu_pipeline, which requires no network or binary).
"""

import pytest

# ---------------------------------------------------------------------------
# Import surface
# ---------------------------------------------------------------------------


class TestApiImports:
    """All three wrappers must be importable from aquamatch.api."""

    def test_run_insitu_pipeline_importable(self):
        from aquamatch.api import run_insitu_pipeline  # noqa: F401

    def test_run_sentinel_pipeline_importable(self):
        from aquamatch.api import run_sentinel_pipeline  # noqa: F401

    def test_run_acolite_pipeline_importable(self):
        from aquamatch.api import run_acolite_pipeline  # noqa: F401

    def test_star_import_works(self):
        """from aquamatch.api import * must expose all three names."""
        import importlib

        api = importlib.import_module("aquamatch.api")
        for name in (
            "run_insitu_pipeline",
            "run_sentinel_pipeline",
            "run_acolite_pipeline",
        ):
            assert hasattr(api, name)


# ---------------------------------------------------------------------------
# Identity — re-exports must be the same objects as in their home modules
# ---------------------------------------------------------------------------


class TestApiIdentity:
    """api.py must re-export the exact same function objects, not copies."""

    def test_run_insitu_pipeline_is_same_object(self):
        from aquamatch.api import run_insitu_pipeline as api_fn
        from aquamatch.insitu_data import run_insitu_pipeline as home_fn

        assert api_fn is home_fn

    def test_run_sentinel_pipeline_is_same_object(self):
        from aquamatch.api import run_sentinel_pipeline as api_fn
        from aquamatch.sentinel_data import run_sentinel_pipeline as home_fn

        assert api_fn is home_fn

    def test_run_acolite_pipeline_is_same_object(self):
        from aquamatch.api import run_acolite_pipeline as api_fn
        from aquamatch.acolite_spec import run_acolite_pipeline as home_fn

        assert api_fn is home_fn


# ---------------------------------------------------------------------------
# __all__
# ---------------------------------------------------------------------------


class TestApiAll:
    """__all__ must be defined and contain exactly the three public names."""

    def test_all_is_defined(self):
        import aquamatch.api as api

        assert hasattr(api, "__all__")

    def test_all_contains_run_insitu_pipeline(self):
        from aquamatch.api import __all__

        assert "run_insitu_pipeline" in __all__

    def test_all_contains_run_sentinel_pipeline(self):
        from aquamatch.api import __all__

        assert "run_sentinel_pipeline" in __all__

    def test_all_contains_run_acolite_pipeline(self):
        from aquamatch.api import __all__

        assert "run_acolite_pipeline" in __all__

    def test_all_contains_exactly_three_names(self):
        from aquamatch.api import __all__

        assert len(__all__) == 3


# ---------------------------------------------------------------------------
# Smoke test — api import produces identical result to home module import
# ---------------------------------------------------------------------------


class TestApiSmoke:
    """
    Calling a wrapper via aquamatch.api must produce the same result as
    calling it via its home module.

    run_insitu_pipeline is used because it requires only disk I/O
    (no network, no binary), making it suitable for a full end-to-end check.
    """

    def _write_stations(self, path):
        import pandas as pd

        pd.DataFrame(
            {
                "codigo_pto": ["P1"],
                "id_estacion": ["E1"],
                "latitud": [-32.85],
                "longitud": [-56.57],
            }
        ).to_excel(path, index=False)

    def _write_campaigns(self, path):
        import pandas as pd

        pd.DataFrame(
            {
                "id_muestra": ["M1"],
                "codigo_pto": ["P1"],
                "id_estacion": ["E1"],
                "fecha_muestra": ["2024-03-15"],
                "observaciones": ["none"],
                "param": ["turbidity"],
                "nombre_clave": ["turb"],
                "parametro": ["Turbidez"],
                "grupo": ["fisico"],
                "uni_nombre": ["NTU"],
                "valor_original": ["1.5"],
                "limite_deteccion": [0.1],
                "limite_cuantificacion": [0.2],
                "valor_transformado": [1.5],
            }
        ).to_excel(path, index=False)

    def test_api_and_home_module_return_same_status(self, tmp_path):
        from aquamatch.api import run_insitu_pipeline as api_fn
        from aquamatch.insitu_data import run_insitu_pipeline as home_fn

        stations = tmp_path / "stations.xlsx"
        campaigns = tmp_path / "campaigns.xlsx"
        self._write_stations(stations)
        self._write_campaigns(campaigns)

        result_api = api_fn(
            stations=stations,
            campaigns=campaigns,
            output_campaigns_csv=tmp_path / "api_campaigns.csv",
            output_unique_csv=tmp_path / "api_unique.csv",
        )
        result_home = home_fn(
            stations=stations,
            campaigns=campaigns,
            output_campaigns_csv=tmp_path / "home_campaigns.csv",
            output_unique_csv=tmp_path / "home_unique.csv",
        )

        assert result_api["status"] == result_home["status"]
        assert result_api["step"] == result_home["step"]
        assert result_api["outputs"]["n_unique"] == result_home["outputs"]["n_unique"]
