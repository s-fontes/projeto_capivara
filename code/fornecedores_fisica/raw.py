import os
from multiprocessing import Pool

import dotenv
import polars as pl
from code.common.utils import process_cpf

dotenv.load_dotenv()

BRONZE_CONTRATOS = '../../data/contratos/bronze/contratos.parquet'
URL_DADOS_ABERTOS = "https://compras.dados.gov.br/fornecedores/doc/fornecedor_pf/"
TOKEN_DADOS_ABERTOS = os.getenv("TOKEN_DADOS_ABERTOS")
BASE_FOLDER = '../../data/fornecedores_fisica/raw'

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
