import argparse
import logging
from pathlib import Path
from re import sub
from typing import Optional

import mgrs
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Maximum number of columns allowed in the campaigns spreadsheet before
# we consider it to be in wide format (the long-format export has ~14 cols).
MAX_CAMPAIGNS_COLUMNS = 35
m = mgrs.MGRS()


def setup_names(excel_path: Path) -> tuple[str, str]:
    """Extrai 'source' e 'station_name' a partir do nome do arquivo Excel."""
    parts = excel_path.stem.split("_")
    if len(parts) < 4:
        logger.warning(f"Nome inesperado para arquivo: {excel_path.name}")
        return "Unknown", excel_path.stem
    source = parts[1]
    station_name = sub(r"\s+", "_", parts[3])
    return source, station_name


def read_and_tag_excel(excel_path: Path) -> pd.DataFrame:
    """Lê um Excel e adiciona colunas de 'Station' e 'Source'."""
    source, station_name = setup_names(excel_path)
    logger.info(f"Lendo {excel_path}...")
    df = pd.read_excel(excel_path)
    df["Station"] = station_name
    df["Source"] = source
    return df


def add_coordinates(df: pd.DataFrame, stations_coords: dict) -> pd.DataFrame:
    """Adiciona colunas de latitude e longitude ao DataFrame."""
    df[["lat", "lon"]] = df.apply(
        lambda row: pd.Series(
            stations_coords.get((row["Source"], row["Station"]), (None, None))
        ),
        axis=1,
    )
    return df


def build_final_csv(
    input_dir: Path,
    output_file: Path,
    stations_coords: dict,
    pattern: str = "Descarga*.xlsx",
) -> None:
    """Concatena arquivos Excel, adiciona coordenadas e salva em CSV."""
    if output_file.exists():
        logger.info(f"Arquivo final já existe: {output_file}")
        return

    logger.info("Concatenando dados das estações automáticas...")
    xlsx_list = list(input_dir.glob(pattern))
    if not xlsx_list:
        logger.warning("Nenhum arquivo Excel encontrado!")
        return

    final_df = pd.concat(
        [read_and_tag_excel(excel_file) for excel_file in xlsx_list],
        ignore_index=True,
    )

    logger.info("Adicionando coordenadas...")
    final_df = add_coordinates(final_df, stations_coords)

    final_df.to_csv(output_file, index=False)
    logger.info(f"CSV final salvo em {output_file}")


def read_stations(station_path: Path) -> pd.DataFrame:
    """Lê o CSV final e retorna um DataFrame."""
    if not station_path.exists():
        logger.error(f"Arquivo CSV não encontrado: {station_path}")
        return pd.DataFrame()
    elif station_path.name.endswith(".xlsx"):
        stations = pd.read_excel(station_path)
    else:
        stations = pd.read_csv(station_path)

    stations = pd.DataFrame(
        stations, columns=["codigo_pto", "id_estacion", "latitud", "longitud"]
    )
    # Assign Sentinel-2 scene
    logger.info(f"Atribuindo cenas Sentinel-2 a partir de coordenadas...")
    stations["s2_tile"] = stations.apply(
        lambda row: get_s2_tile(
            row["latitud"],
            row["longitud"],
        ),
        axis=1,
    )
    return stations


def read_campaigns(campaigns_path: Path) -> pd.DataFrame:
    """
    Lê e valida o arquivo de campanhas.

    Raises
    ------
    ValueError
        Se o arquivo contiver mais de MAX_CAMPAIGNS_COLUMNS colunas,
        indicando que foi exportado no formato largo (wide) em vez do
        formato longo (long) requerido.
    """
    if not campaigns_path.exists():
        logger.error(f"Arquivo CSV não encontrado: {campaigns_path}")
        return pd.DataFrame()
    elif campaigns_path.name.endswith(".xlsx"):
        campaigns = pd.read_excel(campaigns_path)
    else:
        campaigns = pd.read_csv(campaigns_path)

    # --- Validação de formato ---
    if len(campaigns.columns) > MAX_CAMPAIGNS_COLUMNS:
        raise ValueError(
            f"O arquivo de campanhas '{campaigns_path.name}' contém "
            f"{len(campaigns.columns)} colunas, o que indica que foi exportado "
            f"no formato LARGO (wide format). "
            f"Por favor, faça o download novamente selecionando o formato "
            f"LONGO (long format) no site do OAN. "
            f"O formato longo possui no máximo {MAX_CAMPAIGNS_COLUMNS} colunas."
        )

    campaigns = pd.DataFrame(
        campaigns,
        columns=[
            "id_muestra",
            "codigo_pto",
            "id_estacion",
            "fecha_muestra",
            "observaciones",
            "param",
            "nombre_clave",
            "parametro",
            "grupo",
            "uni_nombre",
            "valor_original",
            "limite_deteccion",
            "limite_cuantificacion",
            "valor_transformado",
        ],
    )
    return campaigns


def clean_value(
    val,
    limite_deteccion=None,
    limite_cuantificacion=None,
) -> Optional[float]:
    """
    Limpa e converte um valor de medição para float.

    Regras de substituição (aplicadas antes da conversão numérica):
      - ``<LD``            → limite_deteccion
      - ``<LC``            → limite_cuantificacion
      - ``LD<X<LC``        → limite_cuantificacion
      - ``<X`` ou ``>X``   → valor numérico X (strip dos símbolos < / >)
      - vírgula decimal    → substituída por ponto

    Parameters
    ----------
    val:
        Valor original da coluna ``valor_original``.
    limite_deteccion:
        Valor do limite de detecção para esta linha (usado quando val == '<LD').
    limite_cuantificacion:
        Valor do limite de quantificação para esta linha
        (usado quando val == '<LC' ou 'LD<X<LC').

    Returns
    -------
    float or None
        Valor numérico convertido, ou None se a conversão falhar.
    """
    import re

    if pd.isna(val):
        return None

    # Already numeric — return directly
    if isinstance(val, (int, float)):
        return float(val)

    val_str = str(val).strip()

    # --- Symbolic substitutions that require context columns ---
    if val_str == "<LD":
        if limite_deteccion is not None and not pd.isna(limite_deteccion):
            return float(limite_deteccion)
        return None

    if val_str == "<LC":
        if limite_cuantificacion is not None and not pd.isna(limite_cuantificacion):
            return float(limite_cuantificacao := limite_cuantificacion)
        return None

    if re.fullmatch(r"LD\s*<\s*X\s*<\s*LC", val_str, flags=re.IGNORECASE):
        if limite_cuantificacion is not None and not pd.isna(limite_cuantificacion):
            return float(limite_cuantificacion)
        return None

    # --- Numeric strings with leading < or > ---
    val_clean = val_str.replace("<", "").replace(">", "").replace(",", ".")
    try:
        return float(val_clean)
    except ValueError:
        return None


def clean_campaigns(campaigns: pd.DataFrame) -> pd.DataFrame:
    """
    Limpa o DataFrame de campanhas substituindo valores simbólicos e
    normalizando a coluna 'organized_value'.

    A lógica de substituição (<LD, <LC, LD<X<LC) está centralizada em
    ``clean_value``, que recebe os valores dos limites por linha.

    The ``fecha_muestra → date`` rename is delegated to
    :func:`_normalize_date_column` so the logic stays in one place.
    """
    campaigns = _normalize_date_column(campaigns)

    campaigns["organized_value"] = campaigns.apply(
        lambda row: clean_value(
            row["valor_original"],
            limite_deteccion=row.get("limite_deteccion"),
            limite_cuantificacion=row.get("limite_cuantificacion"),
        ),
        axis=1,
    )
    return campaigns


def merge_stations_campaigns(
    stations: pd.DataFrame, campaigns: pd.DataFrame
) -> pd.DataFrame:
    """Faz o merge entre estações e campanhas."""
    merged_df = pd.merge(
        campaigns,
        stations,
        how="left",
        on="id_estacion",
    )
    return merged_df


def get_s2_tile(lat, lon):
    mgrs_code = m.toMGRS(lat, lon)

    # Sentinel-2 tile
    return mgrs_code[:5]


def remove_duplicate_records(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Remove registros duplicados baseando-se no par (date, sentinel2_scene).

    A primeira ocorrência de cada par é mantida; as demais são consideradas
    duplicatas e removidas.  O conjunto de duplicatas pode ser salvo em
    CSV para auditoria.

    Parameters
    ----------
    df:
        DataFrame contendo ao menos as colunas ``date`` e
        ``sentinel2_scene``.
    duplicates_output:
        Caminho opcional para salvar o CSV com os registros removidos.

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        (df_clean, df_duplicates) — DataFrame sem duplicatas e DataFrame
        com os registros removidos.
    """
    required = {"date", "s2_tile"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Colunas necessárias para remoção de duplicatas ausentes: {missing}"
        )

    duplicated_mask = df.duplicated(subset=["date", "s2_tile"], keep="first")
    df_clean = df[~duplicated_mask].reset_index(drop=True)

    logger.info(
        f"Remoção de duplicatas: {len(df_clean)} registros mantidos, "
        f"(critério: date + s2_tile)."
    )

    return df_clean


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _normalize_date_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure the date column is named ``date`` and contains ``datetime64`` values.

    When the raw export from OAN still carries the original Spanish column name
    ``fecha_muestra``, this function renames it and parses its values.  If
    ``date`` already exists the DataFrame is returned unchanged.

    This is the single canonical implementation of the rename logic — both
    :func:`clean_campaigns` (full cleaning path) and
    :func:`run_insitu_pipeline` (``skip_clean=True`` path) delegate here so
    the behaviour never diverges.

    Parameters
    ----------
    df:
        Campaigns DataFrame as returned by :func:`read_campaigns`.

    Returns
    -------
    pd.DataFrame
        DataFrame with a ``date`` column of type ``datetime64[ns]``.
    """
    if "fecha_muestra" in df.columns and "date" not in df.columns:
        logger.info("Renaming 'fecha_muestra' → 'date' and parsing as datetime.")
        df = df.copy()
        df["fecha_muestra"] = pd.to_datetime(df["fecha_muestra"], errors="coerce")
        df = df.rename(columns={"fecha_muestra": "date"})
    return df


# ---------------------------------------------------------------------------
# Public pipeline wrapper
# ---------------------------------------------------------------------------


def run_insitu_pipeline(
    stations: "Path | str | None" = None,
    campaigns: "Path | str | None" = None,
    output_campaigns_csv: "Path | str | None" = None,
    output_unique_csv: "Path | str | None" = None,
    skip_clean: bool = False,
) -> dict:
    """
    Run the full in situ campaigns pipeline and return a status dict.

    This is the importable equivalent of::

        python -m aquamatch.insitu_data --mode campaigns

    It orchestrates :func:`read_stations`, :func:`read_campaigns`,
    :func:`clean_campaigns`, :func:`merge_stations_campaigns`, and
    :func:`remove_duplicate_records`, then writes two CSV outputs:

    * **campaigns CSV** — full merged table (``observaciones`` column dropped).
    * **unique CSV** — deduplicated records with columns
      ``[date, latitud, longitud, s2_tile]``, suitable as input to
      :func:`~aquamatch.sentinel_data.run_sentinel_pipeline`.

    Parameters
    ----------
    stations:
        Path to the stations file (``.xlsx`` or ``.csv``).
        Defaults to ``data/original_data/estaciones-seleccionadas.xlsx``.
    campaigns:
        Path to the campaigns export file (``.xlsx`` or ``.csv``).
        Must be in **long format** (≤ 35 columns). Defaults to
        ``data/original_data/campaigns_sample.xlsx``.
    output_campaigns_csv:
        Destination path for the merged campaigns CSV.
        Defaults to ``data/monitoring_data/campaigns_organized.csv``.
    output_unique_csv:
        Destination path for the deduplicated unique-records CSV.
        Defaults to ``data/monitoring_data/campaigns_unique_data.csv``.
    skip_clean:
        If ``True``, skip :func:`clean_campaigns` (value cleaning and
        ``organized_value`` computation).  Use when the OAN export has
        already been cleaned upstream.  The ``fecha_muestra → date``
        rename is still applied regardless of this flag.

    Returns
    -------
    dict
        ``{"step": "insitu", "status": "success", "outputs": {...},
        "error": None, "elapsed_seconds": float}``

        On failure the dict has ``"status": "error"`` and the exception
        message in ``"error"``.  ``outputs`` keys:

        * ``n_merged`` — total rows in the merged DataFrame.
        * ``n_unique`` — rows written to the unique-records CSV
          (after duplicate removal on ``date + s2_tile``).
        * ``campaigns_csv`` — resolved path of the campaigns output.
        * ``csv`` — resolved path of the unique-records output.

    Raises
    ------
    RuntimeError
        If either the stations or campaigns DataFrame is empty after
        reading (file not found, unreadable, or all rows filtered out).

    Examples
    --------
    Using explicit paths::

        from aquamatch.insitu_data import run_insitu_pipeline

        result = run_insitu_pipeline(
            stations="data/original_data/my_stations.xlsx",
            campaigns="data/original_data/my_export.xlsx",
        )
        print(result["outputs"]["n_unique"])

    Relying on project defaults::

        result = run_insitu_pipeline()

    Skipping value cleaning (pre-cleaned export)::

        result = run_insitu_pipeline(
            campaigns="data/original_data/clean_export.xlsx",
            skip_clean=True,
        )
    """
    import time

    # --- Resolve defaults from InsituSection to keep a single source of truth ---
    from aquamatch.pipeline_config import InsituSection

    _defaults = InsituSection()

    stations_path = (
        Path(stations) if stations is not None else Path(_defaults.stations_path)
    )
    campaigns_path = (
        Path(campaigns) if campaigns is not None else Path(_defaults.campaigns_path)
    )
    out_campaigns = (
        Path(output_campaigns_csv)
        if output_campaigns_csv is not None
        else Path(_defaults.output_campaigns_csv)
    )
    out_unique = (
        Path(output_unique_csv)
        if output_unique_csv is not None
        else Path(_defaults.output_unique_csv)
    )

    t0 = time.monotonic()

    try:
        # --- Read ---
        stations_df = read_stations(stations_path)
        campaigns_df = read_campaigns(campaigns_path)

        if stations_df.empty or campaigns_df.empty:
            raise RuntimeError(
                "Empty DataFrame after reading inputs — check that both files exist "
                f"and are non-empty.\n  stations:  {stations_path}\n"
                f"  campaigns: {campaigns_path}"
            )

        # --- Clean / normalise date column ---
        if not skip_clean:
            logger.info("Running clean_campaigns...")
            campaigns_df = clean_campaigns(campaigns_df)
        else:
            logger.info("Skipping clean_campaigns (skip_clean=True).")
            campaigns_df = _normalize_date_column(campaigns_df)

        # --- Merge ---
        merged_df = merge_stations_campaigns(stations_df, campaigns_df)

        # --- Write campaigns CSV ---
        out_campaigns.parent.mkdir(parents=True, exist_ok=True)
        merged_df.drop(columns="observaciones", errors="ignore").to_csv(
            out_campaigns, index=False
        )
        logger.info(f"Campaigns CSV saved: {out_campaigns}")

        # --- Deduplicate and write unique CSV ---
        df_clean = remove_duplicate_records(merged_df)
        df_unique = pd.DataFrame(
            df_clean, columns=["date", "latitud", "longitud", "s2_tile"]
        )
        out_unique.parent.mkdir(parents=True, exist_ok=True)
        df_unique.to_csv(out_unique, index=False)
        logger.info(f"Unique records CSV saved: {out_unique}  ({len(df_unique)} rows)")

        return {
            "step": "insitu",
            "status": "success",
            "outputs": {
                "n_merged": len(merged_df),
                "n_unique": len(df_unique),
                "campaigns_csv": out_campaigns,
                "csv": out_unique,
            },
            "error": None,
            "elapsed_seconds": round(time.monotonic() - t0, 2),
        }

    except Exception as exc:
        logger.error(f"run_insitu_pipeline failed: {exc}")
        return {
            "step": "insitu",
            "status": "error",
            "outputs": {},
            "error": str(exc),
            "elapsed_seconds": round(time.monotonic() - t0, 2),
        }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pipeline to organize OAN in situ water quality monitoring data"
    )
    parser.add_argument(
        "--mode",
        choices=["realtime", "campaigns"],
        required=True,
        help="What kind of data: OAN real time or field campaigns",
    )
    parser.add_argument(
        "--skip-clean",
        action="store_true",
        default=False,
        help=(
            "Skip the clean_campaigns step. Use this flag when the data "
            "has already been cleaned by OAN before export."
        ),
    )
    parser.add_argument(
        "--stations",
        type=Path,
        default=Path("./data/original_data/estaciones-seleccionadas.xlsx"),
        help="Path to the stations file (.xlsx or .csv). Only used in campaigns mode.",
    )
    parser.add_argument(
        "--campaigns",
        type=Path,
        default=Path("./data/original_data/campaigns_sample.xlsx"),
        help="Path to the campaigns file (.xlsx or .csv). Only used in campaigns mode.",
    )
    FINAL_PATH = Path("./data/monitoring_data/Automatic_WQ_monitoring_stations.csv")
    args = parser.parse_args()

    if args.mode == "realtime":
        INPUT_DIR = FINAL_PATH.parent

        stations_coords = {
            ("Blanvira", "Boya_Blanvira"): (-32.840556, -56.570278),
            ("Blanvira", "Rincon_del_Bonete"): (-32.829722, -56.418889),
            ("Blanvira", "Baygorria"): (-32.879167, -56.802500),
        }

        logger.info("Iniciando script de organização de dados...")
        build_final_csv(INPUT_DIR, FINAL_PATH, stations_coords)

    elif args.mode == "campaigns":
        result = run_insitu_pipeline(
            stations=args.stations,
            campaigns=args.campaigns,
            skip_clean=args.skip_clean,
        )
        if result["status"] != "success":
            logger.error(f"Pipeline failed: {result['error']}")
