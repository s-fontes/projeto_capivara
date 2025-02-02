import os
from datetime import datetime, timedelta
from multiprocessing import Pool

import polars as pl
import requests

CONTRATOS_URL = 'https://pncp.gov.br/api/consulta/v1/orgaos/'
BASE_FOLDER = '../../data/pncp_contratos/raw'

schema = pl.Schema(
    {
        "numeroControlePncpCompra": pl.String(),
        "codigoPaisFornecedor": pl.String(),
        "anoContrato": pl.String(),
        "tipoContrato.id": pl.String(),
        "tipoContrato.nome": pl.String(),
        "numeroContratoEmpenho": pl.String(),
        "dataAssinatura": pl.String(),
        "dataVigenciaInicio": pl.String(),
        "dataVigenciaFim": pl.String(),
        "niFornecedor": pl.String(),
        "tipoPessoa": pl.String(),
        "orgaoEntidade.cnpj": pl.String(),
        "orgaoEntidade.razaoSocial": pl.String(),
        "orgaoEntidade.poderId": pl.String(),
        "orgaoEntidade.esferaId": pl.String(),
        "categoriaProcesso.id": pl.String(),
        "categoriaProcesso.nome": pl.String(),
        "dataPublicacaoPncp": pl.String(),
        "dataAtualizacao": pl.String(),
        "sequencialContrato": pl.String(),
        "unidadeOrgao.ufNome": pl.String(),
        "unidadeOrgao.ufSigla": pl.String(),
        "unidadeOrgao.municipioNome": pl.String(),
        "unidadeOrgao.codigoUnidade": pl.String(),
        "unidadeOrgao.nomeUnidade": pl.String(),
        "unidadeOrgao.codigoIbge": pl.String(),
    }
)


def get_page(url: str, data_inicial: str, data_final: str, pagina: str, retries: int = 3) -> dict:
    try:
        response = requests.get(f'{url}dataInicial={data_inicial}&dataFinal={data_final}&pagina={pagina}')
        response.raise_for_status()
        return response.json()
    except Exception as e:
        if retries > 0:
            return get_page(url, data_inicial, data_final, pagina, retries - 1)
        raise e


def save_page(data: dict, filename: str) -> None:
    df = pl.json_normalize(data.get('data', {}), schema=schema)
    if not os.path.exists(BASE_FOLDER):
        os.makedirs(BASE_FOLDER)
    df.write_parquet(filename)


def process_page(url: str, data_inicial: str, data_final: str, pagina: str, base_folder: str) -> None:
    if not os.path.exists(base_folder):
        os.makedirs(base_folder)
    filename = os.path.join(base_folder, f'{data_inicial}_{data_final}_{pagina}.parquet')
    if os.path.exists(filename):
        print(f'File: {filename} - Already exists')
        return
    try:
        data = get_page(url, data_inicial, data_final, pagina)
        save_page(data, filename)
        print(f'File: {filename} - Success')
    except Exception as e:
        print(f'File: {filename} - Error: {e}')


def process_date_range(url: str, data_inicial: str, data_final: str, base_folder: str, reprocess: bool = False) -> None:
    pages = get_page(url, data_inicial, data_final, '1').get('totalPaginas')
    if pages is None:
        raise ValueError('Could not get the number of pages')
    offset_list = list(range(1, pages + 1))
    with Pool(8) as pool:
        pool.starmap(process_page,
                     [(url, data_inicial, data_final, base_folder, str(pagina)) for pagina in offset_list])


if __name__ == '__main__':
    initial_date = datetime(2021, 1, 1)
    final_date = datetime.now()
    date_window = timedelta(days=365)
    while initial_date < final_date:
        i_data = initial_date.strftime('%Y%m%d')
        f_data = (initial_date + date_window).strftime('%Y%m%d')
        process_date_range(CONTRATOS_URL, i_data, f_data, BASE_FOLDER)
        initial_date += date_window
