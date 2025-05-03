import polars as pl
import os

from concurrent.futures import ThreadPoolExecutor

import sys
sys.path.append(os.path.abspath(__file__ + "/../../.."))
from common import get_path
from schemas import schema_despesas, schema_receitas


SOURCE_FOLDER = get_path("/data/prestacao_contas_anual_partidaria/raw/")
TARGET_FOLDER = get_path("/data/prestacao_contas_anual_partidaria/bronze/")


def read_csv(file: str, schema: pl.Schema) -> pl.DataFrame:
    return pl.read_csv(
        file,
        schema=schema,
        has_header=True,
        separator=";",
        null_values=["", "#NULO#"],
    )


def get_df(files: list[str], schema: pl.Schema) -> pl.DataFrame:
    with ThreadPoolExecutor() as executor:
        dataframes = list(executor.map(lambda file: read_csv(file, schema), files))
    return pl.concat(dataframes)


def save_parquet(df: pl.DataFrame, file_name: str) -> None:
    df.write_parquet(
        os.path.join(TARGET_FOLDER, file_name),
        compression="gzip"
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

    csv_files_despesa = [
        file for file in csv_files
        if "despesa_" in file
    ]

    csv_files_receita = [
        file for file in csv_files
        if "receita_" in file
    ]
    print("CSV files read successfully")

    print("Processing files...")
    df_despesas = get_df(csv_files_despesa, schema_despesas())
    df_receitas = get_df(csv_files_receita, schema_receitas())
    print("Files processed successfully")

    print("Saving prestacao_contas_anual_partidaria to Parquet files...")
    save_parquet(df_despesas, "despesas.parquet")
    save_parquet(df_receitas, "receitas.parquet")
    print("prestacao_contas_anual_partidaria saved successfully")

    print("prestacao_contas_anual_partidaria data process completed successfully")


if __name__ == "__main__":
    print("Debugging prestacao_contas_anual_partidaria...")
    main()
    print("Debugging finished")