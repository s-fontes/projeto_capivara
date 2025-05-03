import os
import polars as pl

import sys
sys.path.append(os.path.abspath(__file__ + "/../../.."))

from common import get_path, normalize_column

CONTRATOS = get_path("/data/contratos/bronze/contratos.parquet")
SOCIO = get_path("/data/other/socio.parquet")
DOADOR_ORIGINARIO = get_path("/data/prestacao_de_contas_eleitorais_candidatos/bronze/receitas_doador_originario")
SOCIO_DOADOR_CANDIDATO = get_path("/data/flags/socio_doador_candidato.parquet")


def main():

    print("Starting socio_politico data process...")

    if not os.path.isdir(os.path.dirname(SOCIO_DOADOR_CANDIDATO)):
        os.makedirs(os.path.dirname(SOCIO_DOADOR_CANDIDATO))

    print(f"Loading data from '{CONTRATOS}'")
    df_contratos = pl.read_parquet(CONTRATOS).with_columns(
        cnpj_cpf=pl.col("fornecedor_cnpj_cpf_idgener").str.replace_all(
            r"\D", ""
        )
    ).select(
        [
            pl.col("id").cast(pl.UInt32),
            pl.col("cnpj_cpf")
        ]
    )

    print(f"Loading data from '{SOCIO}'")
    df_socio = pl.read_parquet(SOCIO).with_columns(
        cnpj_cpf=pl.col("cnpj_cpf_socio").str.replace_all(
            r"\D", ""
        ),
        nome=normalize_column("nome_socio")
    ).select(
        [
            pl.col("cnpj"),
            pl.col("cnpj_cpf"),
            pl.col("nome")
        ]
    )

    print(f"Loading data from '{DOADOR_ORIGINARIO}'")
    df_doador_originario = pl.read_parquet(DOADOR_ORIGINARIO).with_columns(
        cnpj_cpf=pl.col("NR_CPF_CNPJ_DOADOR_ORIGINARIO").str.pad_start(
            11, "0"
        ).str.slice(3, 6),
        nome=normalize_column("NM_DOADOR_ORIGINARIO_RFB")
    ).select(
        [
            pl.col("cnpj_cpf"),
            pl.col("nome")
        ]
    )

    print("All data loaded successfully")

    print("Initializing SQLContext")
    ctx = pl.SQLContext(
        contratos=df_contratos,
        socio=df_socio,
        doador_originario=df_doador_originario,
        eager=True
    )
    print("SQLContext initialized successfully")

    print("Executing SQL query")
    socio_doador_candidato = ctx.execute(
        """
            select
                c.id,
                case
                    when sum(
                        case
                            when do.nome is null then 0
                            else 1
                        end
                    ) > 0 then true
                    else false
                end as socio_doador_candidato
            from
                contratos as c
            left join socio as s on
                c.cnpj_cpf = s.cnpj
            left join doador_originario as do on
                do.nome = s.nome and
                do.cnpj_cpf = s.cnpj_cpf
            group by c.id
        """
    )

    print("SQL query executed successfully")
    print(socio_doador_candidato)
    print(f"Saving socio_doador_candidato to '{SOCIO_DOADOR_CANDIDATO}'")
    socio_doador_candidato.write_parquet(
        SOCIO_DOADOR_CANDIDATO,
        compression="gzip"
    )
    print("socio_doador_candidato saved successfully")


if __name__ == "__main__":
    print("Debuging socio_politico...")
    main()
    print("Debugging finished")
