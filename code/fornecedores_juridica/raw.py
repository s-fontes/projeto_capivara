import json
import os
from multiprocessing import Pool

import dotenv
import polars as pl
import requests

from code.common.utils import save_json

dotenv.load_dotenv()

BRONZE_CONTRATOS = '../../data/contratos/bronze/contratos.parquet'
URL_DADOS_ABERTOS = "https://compras.dados.gov.br/fornecedores/doc/fornecedor_pj"
BASE_FOLDER = '../../data/fornecedores_juridica/raw'


def validate_cnjp_file(file_path: str) -> bool:
    if not os.path.exists(file_path):
        return False
    with open(file_path, 'r') as file:
        try:
            data = json.load(file)
        except Exception as e:
            return False
        if os.path.getsize(file_path) == 0:
            return False
        elif json.dumps(data) in ["{}", "[]"]:
            return False
        elif data.get("error", None) is not None:
            return False
        return True


def get_cnpj_data(url: str, cnpj: str, retries: int = 3) -> dict:
    try:
        response = requests.get(f"{url}/{cnpj}.json")
        response.raise_for_status()
        return response.json()
    except Exception as e:
        if retries > 0:
            return get_cnpj_data(url, cnpj, retries - 1)
        raise e


def process_cnpj(url: str, cnpj: str, base_folder: str) -> None:
    if not os.path.exists(base_folder):
        os.makedirs(base_folder)
    filename = os.path.join(base_folder, f'{cnpj}.json')
    if validate_cnjp_file(filename):
        print(f'CNPJ - {cnpj} - {filename} - Already exists')
        return
    try:
        data = get_cnpj_data(url, cnpj)
        save_json(data, filename)
        print(f'CNPJ - {cnpj} - {filename} - Success')
    except Exception as e:
        print(f'CNPJ - {cnpj} - {filename} - Error: {e}')


if __name__ == '__main__':
    cnpjs_list = pl.read_parquet(
        BRONZE_CONTRATOS
    ).select(
        [
            "fornecedor_cnpj_cpf_idgener",
            "fornecedor_tipo"
        ]
    ).filter(
        pl.col("fornecedor_tipo") == "JURIDICA"
    ).with_columns(
        cnpj=pl.col("fornecedor_cnpj_cpf_idgener").str.replace(r'\D', '', n=-1)
    ).select(
        "cnpj"
    ).to_series(
    ).to_list()
    with Pool(4) as pool:
        pool.starmap(process_cnpj, [(URL_DADOS_ABERTOS, cnpj, BASE_FOLDER) for cnpj in cnpjs_list])
