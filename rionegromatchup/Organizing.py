import argparse
import logging
from pathlib import Path
from re import sub

import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
    return stations


def read_campaigns(campaigns_path: Path) -> pd.DataFrame:
    """Lê o CSV final e retorna um DataFrame."""
    if not campaigns_path.exists():
        logger.error(f"Arquivo CSV não encontrado: {campaigns_path}")
        return pd.DataFrame()
    elif campaigns_path.name.endswith(".xlsx"):
        campaigns = pd.read_excel(campaigns_path)
    else:
        campaigns = pd.read_csv(campaigns_path)

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


def clean_value(val):
    if pd.isna(val):
        return None
    # Remover '<' e trocar ',' por '.'
    val_clean = val.replace("<", "").replace(",", ".")
    try:
        return float(val_clean)
    except ValueError:
        return None


def clean_campaigns(campaigns: pd.DataFrame) -> pd.DataFrame:
    """Limpa o DataFrame de campanhas."""
    campaigns["organized_value"] = campaigns["valor_original"]
    campaigns.loc[campaigns["valor_original"] == "<LD", "organized_value"] = campaigns[
        "limite_cuantificacion"
    ]

    campaigns.loc[campaigns["valor_original"] == "<LC", "organized_value"] = campaigns[
        "limite_cuantificacion"
    ]
    campaigns["organized_value"] = campaigns["organized_value"].apply(clean_value)
    return campaigns


def merge_stations_campaigns(
    stations: pd.DataFrame, campaigns: pd.DataFrame
) -> pd.DataFrame:
    """Faz o merge entre estações e campanhas."""
    merged_df = pd.merge(
        campaigns,
        stations,
        how="left",
        left_on=["codigo_pto", "id_estacion"],
        right_on=["codigo_pto", "id_estacion"],
    )
    return merged_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pipeline to organize OAN water quality monitoring data"
    )
    parser.add_argument(
        "--mode",
        choices=["realtime", "campaigns"],
        required=True,
        help="What king of data: OAN real time or field campaigns",
    )
    FINAL_PATH = Path(
        "./datos/mediciones/OAN_tiempo_real/Automatic_WQ_monitoring_stations.csv"
    )
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
        STATIONS_PATH = Path("./data/estaciones-seleccionadas.xlsx")
        CAMPAIGNS_PATH = Path("./data/extraccion_20250930-181325.xlsx")
        OUTPUT_CAMPAIGNS_PATH = Path(
            "./data//campaigns_organized.csv"
        )

        stations_df = read_stations(STATIONS_PATH)
        campaigns_df = read_campaigns(CAMPAIGNS_PATH)
        if stations_df.empty or campaigns_df.empty:
            logger.error("Erro ao ler os arquivos de estações ou campanhas.")
        else:
            campaigns_df = clean_campaigns(campaigns_df)
            merged_df = merge_stations_campaigns(campaigns_df, stations_df)
            merged_df.to_csv(OUTPUT_CAMPAIGNS_PATH, index=False, sep=';')
            logger.info(f"Campanhas organizadas salvas em {OUTPUT_CAMPAIGNS_PATH}")
