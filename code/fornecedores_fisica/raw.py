import os
from multiprocessing import Pool

import dotenv
import polars as pl
import requests

from code.common.utils import save_json

dotenv.load_dotenv()

BRONZE_CONTRATOS = '../../data/contratos/bronze/contratos.parquet'
URL_DADOS_ABERTOS = "https://compras.dados.gov.br/fornecedores/doc/fornecedor_pf/"
TOKEN_DADOS_ABERTOS = os.getenv("TOKEN_DADOS_ABERTOS")
BASE_FOLDER = '../../data/fornecedores_fisica/raw'


def get_cpf_data(url: str, codigo: str, token: str, retries: int = 3) -> dict:
    try:
        response = requests.get(url, params={"codigo": codigo}, headers={"chave-api-dados": token})
        response.raise_for_status()
        return response.json()
    except Exception as e:
        if retries > 0:
            return get_cpf_data(url, codigo, token, retries - 1)
        raise e


def process_cpf(url: str, codigo: str, token: str, base_folder: str) -> None:
    if not os.path.exists(base_folder):
        os.makedirs(base_folder)
    filename = os.path.join(base_folder, f'{codigo}.json')
    if os.path.exists(filename):
        print(f'CPF - {codigo} - {filename} - Already exists')
        return
    try:
        data = get_cpf_data(url, codigo, token)
        save_json(data, filename)
        print(f'CPF - {codigo} - {filename} - Success')
    except Exception as e:
        print(f'CPF - {codigo} - {filename} - Error: {e}')


if __name__ == '__main__':
    cpfs_list = pl.read_parquet(
        BRONZE_CONTRATOS
    ).select(
        [
            "fornecedor_cnpj_cpf_idgener",
            "fornecedor_tipo"
        ]
    ).filter(
        pl.col("fornecedor_tipo") == "FISICA"
    ).with_columns(
        cpf=pl.col("fornecedor_cnpj_cpf_idgener").str.replace(r'\D', '', n=-1)
    ).select(
        "cpf"
    ).to_series(
    ).to_list()
    with Pool(4) as pool:
        pool.starmap(process_cpf, [(URL_DADOS_ABERTOS, cpf, TOKEN_DADOS_ABERTOS, BASE_FOLDER) for cpf in cpfs_list])
