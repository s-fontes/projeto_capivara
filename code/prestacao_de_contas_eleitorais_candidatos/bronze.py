import polars as pl
import os

from concurrent.futures import ThreadPoolExecutor

import sys
sys.path.append(os.path.abspath(__file__ + "/../../.."))
from common import get_path
from schemas import schema_despesas_contratadas, schema_despesas_pagas, schema_receitas, schema_receitas_doador_originario


SOURCE_FOLDER = get_path("/data/prestacao_de_contas_eleitorais_candidatos/raw/")
TARGET_FOLDER = get_path("/data/prestacao_de_contas_eleitorais_candidatos/bronze/")


def read_csv(file: str, schema: pl.Schema) -> pl.DataFrame:
    print(f"Reading file: {file}")
    return pl.read_csv(
        file,
        schema=schema,
        has_header=True,
        separator=";",
        null_values=["", "#NULO#", "#NULO"],
    )


def get_df(files: list[str], schema: pl.Schema) -> pl.DataFrame:
    with ThreadPoolExecutor() as executor:
        dataframes = list(executor.map(lambda file: read_csv(file, schema), files))
    return pl.concat(dataframes)


def save_parquet_partitioned(df: pl.DataFrame, file_name: str, partition_col: str) -> None:
    for value in df.select(partition_col).unique().to_series():
        print(f"Saving partition: {file_name} - {partition_col}={value}")

        partition_df = df.filter(pl.col(partition_col) == value)

        partition_folder = os.path.join(TARGET_FOLDER, file_name)
        os.makedirs(partition_folder, exist_ok=True)

        partition_df.write_parquet(
            os.path.join(partition_folder, f"{partition_col}_{value}.parquet"),
            row_group_size=1_000_000
        )

def main():

    print("Starting prestacao_contas_anual_partidaria data process...")

    if not os.path.isdir(TARGET_FOLDER):
        os.makedirs(TARGET_FOLDER)

    print("Reading CSV files...")
    csv_files = [
        os.path.join(root, file) for root, _, files in os.walk(SOURCE_FOLDER)
        for file in files
        if file.endswith(".csv")
    ]

    csv_files_despesas_contratadas = [
        file for file in csv_files
        if "despesas_contratadas_" in file
    ]

    csv_files_despesas_pagas = [
        file for file in csv_files
        if "despesas_pagas_" in file
    ]

    csv_files_receitas = [
        file for file in csv_files
        if "receitas_candidatos_" in file and "_doador_originario_" not in file
    ]

    csv_files_receitas_doador_originario = [
        file for file in csv_files
        if "_doador_originario_" in file
    ]
    print("CSV files read successfully")


    print("Saving data to parquet...")

    save_parquet_partitioned(
        get_df(csv_files_despesas_contratadas, schema_despesas_contratadas()),
        "despesas_contratadas",
        "NR_PARTIDO"
    )
    save_parquet_partitioned(
        get_df(csv_files_despesas_pagas, schema_despesas_pagas()),
        "despesas_pagas",
        "AA_ELEICAO"
    )
    save_parquet_partitioned(
        get_df(csv_files_receitas, schema_receitas()),
        "receitas",
        "NR_PARTIDO"
    )
    save_parquet_partitioned(
        get_df(csv_files_receitas_doador_originario, schema_receitas_doador_originario()),
        "receitas_doador_originario",
        "AA_ELEICAO"
    )
    print("Data saved successfully")


if __name__ == "__main__":
    print("Debugging prestacao_contas_anual_partidaria...")
    main()
    print("Debugging finished")