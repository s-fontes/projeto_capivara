import polars as pl


def schema_despesas() -> pl.Schema:
    return pl.Schema(
        {
            "DT_GERACAO": pl.Date(),
            "HH_GERACAO": pl.Time(),
            "AA_EXERCICIO": pl.Int64(),
            "TP_DESPESA": pl.String(),
            "CD_TP_ESFERA_PARTIDARIA": pl.Int64(),
            "DS_TP_ESFERA_PARTIDARIA": pl.String(),
            "SG_UF": pl.String(),
            "CD_MUNICIPIO": pl.Int64(),
            "NM_MUNICIPIO": pl.String(),
            "NR_ZONA": pl.Int64(),
            "NR_CNPJ_PRESTADOR_CONTA": pl.String(),
            "SG_PARTIDO": pl.String(),
            "NM_PARTIDO": pl.String(),
            "CD_TP_DOCUMENTO": pl.String(),
            "DS_TP_DOCUMENTO": pl.String(),
            "NR_DOCUMENTO": pl.String(),
            "AA_AIDF": pl.Int64(),
            "NR_AIDF": pl.Int64(),
            "CD_TP_FORNECEDOR": pl.String(),
            "DS_TP_FORNECEDOR": pl.String(),
            "NR_CPF_CNPJ_FORNECEDOR": pl.String(),
            "NM_FORNECEDOR": pl.String(),
            "DS_GASTO": pl.String(),
            "DT_PAGAMENTO": pl.Date(),
            "VR_GASTO": pl.Decimal(15, 2),
            "VR_PAGAMENTO": pl.Decimal(15, 2),
            "VR_DOCUMENTO": pl.Decimal(15, 2),
            "CD_FONTE_DESPESA": pl.Int64(),
            "DS_FONTE_DESPESA": pl.String(),
            "SQ_DESPESA": pl.Int64()
        }
    )


def schema_receitas() -> pl.Schema:
    return pl.Schema(
        {
            "DT_GERACAO": pl.Date(),
            "HH_GERACAO": pl.Time(),
            "CD_TP_ESFERA_PARTIDARIA": pl.Int64(),
            "DS_TP_ESPERA_PARTIDARIA": pl.String(),
            "SG_UF": pl.String(),
            "CD_MUNICIPIO": pl.Int64(),
            "NM_MUNICIPIO": pl.String(),
            "NR_ZONA": pl.String(),
            "NR_CNPJ_PRESTADOR_CONTA": pl.String(),
            "SG_PARTIDO": pl.String(),
            "NM_PARTIDO": pl.String(),
            "CD_TP_ORIGEM_DOACAO": pl.Int64(),
            "DS_TP_ORIGEM_DOACAO": pl.String(),
            "NR_CPF_CNPJ_DOADOR": pl.String(),
            "NM_DOADOR": pl.String(),
            "CD_TP_ESFERA_PARTIDARIA_DOADOR": pl.Int64(),
            "DS_TP_ESFERA_PARTIDARIA_DOADOR": pl.String(),
            "SG_UF_DOADOR": pl.String(),
            "CD_MUNICIPIO_DOADOR": pl.Int64(),
            "NM_MUNICIPIO_DOADOR": pl.String(),
            "NR_ZONA_DOADOR": pl.String(),
            "SQ_CANDIDATO_DOADOR": pl.String(),
            "NR_CANDIDATO_DOADOR": pl.String(),
            "CD_CANDIDATO_CARGO_DOADOR": pl.Int64(),
            "DS_CANDIDATO_CARGO_DOADOR": pl.String(),
            "CD_TP_FONTE_RECURSO": pl.Int64(),
            "DS_TP_FONTE_RECURSO": pl.String(),
            "CD_TP_NATUREZA_RECURSO": pl.Int64(),
            "DS_TP_NATUREZA_RECURSO": pl.String(),
            "CD_TP_ESPECIE_RECURSO": pl.Int64(),
            "DS_TP_ESPECIE_RECURSO": pl.String(),
            "NR_RECIBO_DOACAO": pl.String(),
            "NR_DOCUMENTO": pl.String(),
            "DT_RECEITA": pl.Date(),
            "DS_RECEITA": pl.String(),
            "VR_RECEITA": pl.Decimal(15, 2)
        }
    )
