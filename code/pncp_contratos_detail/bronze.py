import json
import os
import polars as pl

BRONZE_FOLDER = '../../data/pncp_contratos_detail/bronze'
RAW_FOLDER = '../../data/pncp_contratos_detail/raw'
ERROR_FOLDER = '../../data/pncp_contratos_detail/error'

schema = pl.Schema(
    {
        "valorTotalEstimado": pl.String(),
        "valorTotalHomologado": pl.String(),
        "orcamentoSigilosoCodigo": pl.String(),
        "orcamentoSigilosoDescricao": pl.String(),
        "numeroControlePNCP": pl.String(),
        "linkSistemaOrigem": pl.String(),
        "linkProcessoEletronico": pl.String(),
        "anoCompra": pl.String(),
        "sequencialCompra": pl.String(),
        "numeroCompra": pl.String(),
        "processo": pl.String(),
        "orgaoEntidade.cnpj": pl.String(),
        "orgaoEntidade.razaoSocial": pl.String(),
        "orgaoEntidade.poderId": pl.String(),
        "orgaoEntidade.esferaId": pl.String(),
        "unidadeOrgao.ufNome": pl.String(),
        "unidadeOrgao.ufSigla": pl.String(),
        "unidadeOrgao.municipioNome": pl.String(),
        "unidadeOrgao.codigoUnidade": pl.String(),
        "unidadeOrgao.nomeUnidade": pl.String(),
        "orgaoSubRogado": pl.String(),
        "unidadeSubRogada": pl.String(),
        "modalidadeId": pl.String(),
        "modalidadeNome": pl.String(),
        "justificativaPresencial": pl.String(),
        "modoDisputaId": pl.String(),
        "modoDisputaNome": pl.String(),
        "tipoInstrumentoConvocatorioCodigo": pl.String(),
        "tipoInstrumentoConvocatorioNome": pl.String(),
        "amparoLegal.descricao": pl.String(),
        "amparoLegal.nome": pl.String(),
        "amparoLegal.codigo": pl.String(),
        "objetoCompra": pl.String(),
        "informacaoComplementar": pl.String(),
        "srp": pl.String(),
        "dataPublicacaoPncp": pl.String(),
        "dataAberturaProposta": pl.String(),
        "dataEncerramentoProposta": pl.String(),
        "situacaoCompraId": pl.String(),
        "situacaoCompraNome": pl.String(),
        "existeResultado": pl.String(),
        "dataInclusao": pl.String(),
        "dataAtualizacao": pl.String(),
        "dataAtualizacaoGlobal": pl.String(),
        "usuarioNome": pl.String(),
    }
)

def get_data(path: str) -> pl.DataFrame:
    try:
        with open(path, 'r') as f:
            return pl.json_normalize(json.load(f), schema=schema)
    except Exception as e:
        print(e)
        return pl.json_normalize({}, schema=schema)


def main():
    files = os.listdir(RAW_FOLDER)
    dfs = []
    for file in files:
        dfs.append(get_data(os.path.join(RAW_FOLDER, file)))
    df = pl.concat(dfs)
    if not os.path.isdir(BRONZE_FOLDER):
        os.makedirs(BRONZE_FOLDER)
    print(df)
    df.write_parquet(os.path.join(BRONZE_FOLDER, "pncp_contratos_detail.parquet"))

if __name__ == '__main__':
    main()
