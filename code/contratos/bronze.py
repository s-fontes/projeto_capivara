import json
import os

import polars as pl

RAW_FOLDER = '../../data/contratos/raw'
BRONZE_FOLDER = '../../data/contratos/bronze'

schema = pl.Schema(
    {
        "id": pl.String(),
        "codigo_contrato": pl.String(),
        "numero": pl.String(),
        "receita_despesa": pl.String(),
        "orgao_codigo": pl.String(),
        "orgao_nome": pl.String(),
        "unidade_codigo": pl.String(),
        "unidade_nome_resumido": pl.String(),
        "unidade_nome": pl.String(),
        "unidade_origem_codigo": pl.String(),
        "unidade_origem_nome": pl.String(),
        "codigo_tipo": pl.String(),
        "tipo": pl.String(),
        "categoria": pl.String(),
        "processo": pl.String(),
        "objeto": pl.String(),
        "fundamento_legal": pl.String(),
        "data_assinatura": pl.String(),
        "data_publicacao": pl.String(),
        "vigencia_inicio": pl.String(),
        "vigencia_fim": pl.String(),
        "valor_inicial": pl.String(),
        "valor_global": pl.String(),
        "num_parcelas": pl.String(),
        "valor_parcela": pl.String(),
        "valor_acumulado": pl.String(),
        "fornecedor_tipo": pl.String(),
        "fornecedor_cnpj_cpf_idgener": pl.String(),
        "fornecedor_nome": pl.String(),
        "codigo_compra": pl.String(),
        "modalidade_codigo": pl.String(),
        "modalidade": pl.String(),
        "unidade_compra": pl.String(),
        "licitacao_numero": pl.String(),
        "informacao_complementar": pl.String(),
        "_links.self.href": pl.String(),
        "_links.self.title": pl.String(),
        "_links.licitacao.href": pl.String(),
        "_links.licitacao.title": pl.String(),
        "_links.uasg.href": pl.String(),
        "_links.uasg.title": pl.String()
    }
)

if __name__ == '__main__':
    files = os.listdir(RAW_FOLDER)
    dfs = []
    for file in files:
        with open(os.path.join(RAW_FOLDER, file)) as f:
            data = json.load(f).get("_embedded", {}).get("contratos", {})
            df = pl.json_normalize(data, schema=schema)
            dfs.append(df)
    df = pl.concat(dfs)
    if not os.path.isdir(BRONZE_FOLDER):
        os.makedirs(BRONZE_FOLDER)
    print(df)
    df.write_parquet(os.path.join(BRONZE_FOLDER, 'contratos.parquet'))
