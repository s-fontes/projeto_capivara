import os
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool
import json

import polars as pl
import requests

BRONZE_CONTRATOS = '../../data/pncp_contratos/bronze/pncp_contratos.parquet'
CONTRATOS_URL = 'https://pncp.gov.br/api/consulta/v1/orgaos'
BASE_FOLDER = '../../data/pncp_contratos_detail/raw'
ERROR_FOLDER = '../../data/pncp_contratos_detail/error'
REPROCESS = False


def get_page(cnpj: str, ano: str, sequencial: str, retries: int = 10) -> dict:
    uri = f'{CONTRATOS_URL}/{cnpj}/compras/{ano}/{sequencial}'
    try:
        response = requests.get(
            uri,
            timeout=10
        )
        response.raise_for_status()
        return response.json()

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404 and e.response.json().get("error") == "Not Found":
            return {
                "managed_error": "Nenhum registro encontrado",
                "managed_uri": uri
            }
        else:
            if retries > 0:
                time.sleep(1)
                return get_page(cnpj, ano, sequencial, retries=retries - 1)
            return {
                "managed_error": str(e),
                "managed_uri": uri
            }

    except Exception as e:
        return {
            "managed_error": str(e),
            "managed_uri": uri
        }


def get_year(cnpj: str, ano: str, sequenciais: list[str]) -> list[dict]:
    with ThreadPoolExecutor(max_workers=8) as executor:
        return list(
            executor.map(
                lambda sequencial: get_page(cnpj, ano, sequencial),
                sequenciais
            )
        )


def get_erros(data: list[dict]) -> list[dict]:
    return list(
        filter(
            lambda x: 'managed_error' in x,
            data
        )
    )


def get_success(data: list[dict]) -> list[dict]:
    return list(
        filter(
            lambda x: 'managed_error' not in x,
            data
        )
    )


def save_page(data: list[dict], filename) -> None:
    success = get_success(data)
    errors = get_erros(data)
    len_errors = len(errors)
    len_success = len(success)
    if len_errors > 0:
        with open(os.path.join(ERROR_FOLDER, filename), 'w') as f:
            json.dump(errors, f)
    if len_success > 0:
        with open(os.path.join(BASE_FOLDER, filename), 'w') as f:
            json.dump(success, f)
    if len_success > 0 or len_errors > 0:
        print(f'File: {filename} - Success: {len(success)
                                             } - Errors: {len(errors)}')
    else:
        raise ValueError('No data to save')


def process_year(cnpj: str, ano: str, sequenciais: list[str]) -> None:

    if not os.path.exists(BASE_FOLDER):
        os.makedirs(BASE_FOLDER)
    if not os.path.exists(ERROR_FOLDER):
        os.makedirs(ERROR_FOLDER)

    filename = f"{cnpj}_{ano}.json"

    if (os.path.exists(os.path.join(BASE_FOLDER, filename)) or
            os.path.exists(os.path.join(ERROR_FOLDER, filename))) and not REPROCESS:
        print(f'File: {filename} - Already exists')
        return
    try:
        data = get_year(cnpj, ano, sequenciais)
        save_page(data, filename)
    except Exception as e:
        print(f'File: {filename} - Error: {e}')


if __name__ == '__main__':
    initial = datetime.now()
    rows = pl.read_parquet(BRONZE_CONTRATOS).sql(
        """
            SELECT
                `orgaoEntidade.cnpj` as cnpj,
                anoContrato as ano,
                array_agg(sequencialContrato) as sequenciais
            FROM self
            GROUP BY `orgaoEntidade.cnpj`, anoContrato
        """
    ).rows(
        named=True
    )

    with Pool(8) as pool:
        to_process = []
        for row in rows:
            sequenciais = []
            try:
                cnpj = row['cnpj']
                ano = str(int(row['ano']))
                for sequencial in row['sequenciais']:
                    sequenciais.append(str(int(sequencial)))
                to_process.append((cnpj, ano, sequenciais))
            except Exception as e:
                print(f"Error: {e}")
        pool.starmap(
            process_year,
            to_process
        )

    print(f"Time elapsed: {datetime.now() - initial}")
