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


if __name__ == "__main__":
    FINAL_PATH = Path("./datos/mediciones/OAN_tiempo_real/Automatic_WQ_monitoring_stations.csv")
    INPUT_DIR = FINAL_PATH.parent

    stations_coords = {
        ("Blanvira", "Boya_Blanvira"): (-32.840556, -56.570278),
        ("Blanvira", "Rincon_del_Bonete"): (-32.829722, -56.418889),
        ("Blanvira", "Baygorria"): (-32.879167, -56.802500),
    }

    logger.info("Iniciando script de organização de dados...")
    build_final_csv(INPUT_DIR, FINAL_PATH, stations_coords)
