import json
import os

import polars as pl

RAW_FOLDER = '../../data/fornecedores_juridica/raw'
BRONZE_FOLDER = '../../data/fornecedores_juridica/bronze'

schema = pl.Schema(
    {
        "id": pl.String(),
        "cnpj": pl.String(),
        "razao_social": pl.String(),
        "nome_fantasia": pl.String(),
        "id_unidade_cadastradora": pl.String(),
        "id_natureza_juridica": pl.String(),
        "id_ramo_negocio": pl.String(),
        "id_porte_empresa": pl.String(),
        "id_cnae": pl.String(),
        "id_cnae2": pl.String(),
        "logradouro": pl.String(),
        "numero_logradouro": pl.String(),
        "complemento_logradouro": pl.String(),
        "bairro": pl.String(),
        "id_municipio": pl.String(),
        "cep": pl.String(),
        "caixa_postal": pl.String(),
        "ativo": pl.String(),
        "recadastrado": pl.String(),
        "habilitado_licitar": pl.String()
    }
)

if __name__ == '__main__':
    files = os.listdir(RAW_FOLDER)
    dfs = []
    for file in files:
        with open(os.path.join(RAW_FOLDER, file)) as f:
            if os.path.getsize(os.path.join(RAW_FOLDER, file)) == 0:
                print(f"Empty file: {file}")
                continue
            try:
                data = json.load(f)
            except Exception as e:
                print(f"Error on {file}: {e}")
                continue
            if data.get("error", None) is not None:
                print(f"Error on {file}: {data['error']}")
                continue
            df = pl.json_normalize(data, schema=schema)
            dfs.append(df)
    df = pl.concat(dfs)
    if not os.path.isdir(BRONZE_FOLDER):
        os.makedirs(BRONZE_FOLDER)
    print(df)
    df.write_parquet(os.path.join(BRONZE_FOLDER, 'fornecedores_juridica.parquet'))
