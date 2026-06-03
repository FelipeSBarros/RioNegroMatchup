import pandas as pd
import pytest

from rionegromatchup.insitu_data import (
    setup_names,
    clean_campaigns,
    merge_stations_campaigns,
    clean_value,
)


class TestSetupNames:
    """Tests for setup_names."""

    def test_extracts_source_and_station(self, tmp_path):
        fake_file = tmp_path / "Descarga_Blanvira_2025_Boya.xlsx"
        fake_file.write_text("")
        source, station = setup_names(fake_file)
        assert source == "Blanvira"
        assert station == "Boya"

    def test_warns_on_unexpected_filename(self, tmp_path):
        fake_file = tmp_path / "unexpected.xlsx"
        fake_file.write_text("")
        source, station = setup_names(fake_file)
        assert source == "Unknown"


class TestCleanValue:
    """Tests for clean_value."""

    def test_removes_less_than_sign(self):
        assert clean_value("<0.5") == pytest.approx(0.5)

    def test_replaces_comma_with_dot(self):
        assert clean_value("1,23") == pytest.approx(1.23)

    def test_returns_none_for_nan(self):
        assert clean_value(float("nan")) is None

    def test_returns_none_for_invalid_string(self):
        assert clean_value("nd") is None

    def test_handles_normal_float_string(self):
        assert clean_value("3.14") == pytest.approx(3.14)


class TestCleanCampaigns:
    """Tests for clean_campaigns."""

    def _make_df(self):
        return pd.DataFrame(
            {
                "fecha_muestra": ["2025-01-15", "2025-02-20"],
                "valor_original": ["<LD", "1,5"],
                "limite_deteccion": [0.1, 0.1],
                "limite_cuantificacion": [0.2, 0.2],
            }
        )

    def test_renames_fecha_to_date(self):
        df = clean_campaigns(self._make_df())
        assert "date" in df.columns
        assert "fecha_muestra" not in df.columns

    def test_replaces_LD_with_limite_deteccion(self):
        df = clean_campaigns(self._make_df())
        assert df.loc[0, "organized_value"] == pytest.approx(0.1)

    def test_replaces_LC_with_limite_cuantificacion(self):
        df = clean_campaigns(
            pd.DataFrame(
                {
                    "fecha_muestra": ["2025-03-10"],
                    "valor_original": ["<LC"],
                    "limite_deteccion": [0.1],
                    "limite_cuantificacion": [0.2],
                }
            )
        )
        assert df.loc[0, "organized_value"] == pytest.approx(0.2)

    def test_parses_comma_decimal(self):
        df = clean_campaigns(self._make_df())
        assert df.loc[1, "organized_value"] == pytest.approx(1.5)

    def test_replaces_LD_between_LC_with_limite_cuantificacion(self):
        df = clean_campaigns(
            pd.DataFrame(
                {
                    "fecha_muestra": ["2025-04-01"],
                    "valor_original": ["LD<X<LC"],
                    "limite_deteccion": [0.1],
                    "limite_cuantificacion": [0.2],
                }
            )
        )
        assert df.loc[0, "organized_value"] == pytest.approx(0.2)

    def test_strips_less_than_numeric(self):
        df = clean_campaigns(
            pd.DataFrame(
                {
                    "fecha_muestra": ["2025-05-01"],
                    "valor_original": ["<1,0"],
                    "limite_deteccion": [None],
                    "limite_cuantificacion": [None],
                }
            )
        )
        assert df.loc[0, "organized_value"] == pytest.approx(1.0)

    def test_strips_greater_than_numeric(self):
        df = clean_campaigns(
            pd.DataFrame(
                {
                    "fecha_muestra": ["2025-06-01"],
                    "valor_original": [">2000"],
                    "limite_deteccion": [None],
                    "limite_cuantificacion": [None],
                }
            )
        )
        assert df.loc[0, "organized_value"] == pytest.approx(2000.0)


class TestMergeStationsCampaigns:
    """Tests for merge_stations_campaigns."""

    def test_merge_adds_coordinates(self):
        stations = pd.DataFrame(
            {
                "codigo_pto": ["P1"],
                "id_estacion": ["E1"],
                "latitud": [-32.85],
                "longitud": [-56.5],
            }
        )
        campaigns = pd.DataFrame(
            {
                "codigo_pto": ["P1"],
                "id_estacion": ["E1"],
                "valor_original": ["1.5"],
            }
        )
        merged = merge_stations_campaigns(stations, campaigns)
        assert "latitud" in merged.columns
        assert "longitud" in merged.columns
        assert merged.loc[0, "latitud"] == pytest.approx(-32.85)

    def test_unmatched_campaign_gets_null_coords(self):
        stations = pd.DataFrame(
            {
                "codigo_pto": ["P1"],
                "id_estacion": ["E1"],
                "latitud": [-32.85],
                "longitud": [-56.5],
            }
        )
        campaigns = pd.DataFrame(
            {
                "codigo_pto": ["P99"],
                "id_estacion": ["E99"],
                "valor_original": ["1.5"],
            }
        )
        merged = merge_stations_campaigns(stations, campaigns)
        assert pd.isna(merged.loc[0, "latitud"])
