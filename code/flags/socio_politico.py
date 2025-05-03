import os
import polars as pl

import sys
sys.path.append(os.path.abspath(__file__ + "/../../.."))

from common import get_path, normalize_column

CONTRATOS = get_path("/data/contratos/bronze/contratos.parquet")
SOCIO = get_path("/data/other/socio.parquet")
CANDIDATOS = get_path("/data/other/candidatos.parquet")
SOCIO_POLITICO = get_path("/data/flags/socio_politico.parquet")


def main():

    print("Starting socio_politico data process...")

    if not os.path.isdir(os.path.dirname(SOCIO_POLITICO)):
        os.makedirs(os.path.dirname(SOCIO_POLITICO))

    print(f"Loading data from '{CONTRATOS}'")
    df_contratos = pl.read_parquet(CONTRATOS).with_columns(
        cnpj_cpf=pl.col("fornecedor_cnpj_cpf_idgener").str.replace_all(
            r"\D", ""
        )
    )

    print(f"Loading data from '{SOCIO}'")
    df_socio = pl.read_parquet(SOCIO).with_columns(
        cnpj_cpf_socio=pl.col("cnpj_cpf_socio").str.replace_all(
            r"\D", ""
        ),
        nome_socio=normalize_column("nome_socio")
    )

    print(f"Loading data from '{CANDIDATOS}'")
    df_candidatos = pl.read_parquet(CANDIDATOS).with_columns(
        NR_CPF_CANDIDATO=pl.col("NR_CPF_CANDIDATO").str.pad_start(
            11, "0"
        ).str.slice(3, 6),
        NM_CANDIDATO=normalize_column("NM_CANDIDATO")
    )

    print("All data loaded successfully")

    print("Initializing SQLContext")
    ctx = pl.SQLContext(
        contratos=df_contratos,
        socio=df_socio,
        candidatos=df_candidatos,
        eager=True
    )
    print("SQLContext initialized successfully")

    print("Executing SQL query")
    socio_politico = ctx.execute(
        """
            select
                c.id::uint8,
                case
                    when sum(
                        case
                            when cd.NM_CANDIDATO is null then 0
                            else 1
                        end
                    ) > 0 then true
                    else false
                end as socio_politico
            from
                contratos as c
            left join socio as s on
                c.cnpj_cpf = s.cnpj
            left join candidatos as cd on
                cd.NM_CANDIDATO = s.nome_socio and
                cd.NR_CPF_CANDIDATO = s.cnpj_cpf_socio
            group by c.id
        """
    )
    print("SQL query executed successfully")
    print(socio_politico)
    print(f"Saving socio_politico to '{SOCIO_POLITICO}'")
    socio_politico.write_parquet(
        SOCIO_POLITICO,
        compression="gzip"
    )
    print("socio_politico saved successfully")


if __name__ == "__main__":
    print("Debuging socio_politico...")
    main()
    print("Debugging finished")
