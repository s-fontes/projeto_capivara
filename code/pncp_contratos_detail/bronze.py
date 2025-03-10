import os
import polars as pl

from concurrent.futures import ThreadPoolExecutor

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

error_schema = pl.Schema(
    {
        "managed_error": pl.String(),
        "managed_url": pl.String()
    }
)


if __name__ == '__main__':
    files = os.listdir(RAW_FOLDER)
    input_executor = [(os.path.join(RAW_FOLDER, file), schema)
                      for file in files]
    with ThreadPoolExecutor() as executor:
        dfs = list(
            executor.map(pl.json_normalize, input_executor))
    df = pl.concat(dfs)
    print(df)
    # df.write_parquet(os.path.join(BRONZE_FOLDER, 'pncp_contratos.parquet'))
