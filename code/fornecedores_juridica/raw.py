from multiprocessing import Pool

import dotenv
import polars as pl
from code.common.utils import process_cnpj

dotenv.load_dotenv()

BRONZE_CONTRATOS = '../../data/contratos/bronze/contratos.parquet'
URL_DADOS_ABERTOS = "https://compras.dados.gov.br/fornecedores/doc/fornecedor_pj"
BASE_FOLDER = '../../data/fornecedores_juridica/raw'

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
