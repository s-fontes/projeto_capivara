import os
import polars as pl

import sys
sys.path.append(os.path.abspath(__file__ + "/../../.."))

from common import get_path, normalize_column

CONTRATOS = get_path("/data/contratos/bronze/contratos.parquet")
SOCIO = get_path("/data/other/socio.parquet")
BOLSA_FAMILIA = get_path("/data/other/bolsa_familia_agrupado.parquet")
SOCIO_BOLSA = get_path("/data/flags/socio_bolsa.parquet")


def main():

    print("Starting socio_bolsa data process...")

    if not os.path.isdir(os.path.dirname(SOCIO_BOLSA)):
        os.makedirs(os.path.dirname(SOCIO_BOLSA))

    print(f"Loading data from '{CONTRATOS}'")
    df_contratos = pl.read_parquet(CONTRATOS).with_columns(
        cnpj_cpf = pl.col("fornecedor_cnpj_cpf_idgener").str.replace_all(
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
        cnpj = pl.col("cnpj").str.replace_all(
            r"\D", ""
        ),
        cnpj_cpf = pl.col("cnpj_cpf_socio").str.replace_all(
            r"\D", ""
        ),
        nome = normalize_column("nome_socio")
    ).select(
        [
            pl.col("cnpj"),
            pl.col("cnpj_cpf_socio"),
            pl.col("nome")
        ]
    )

    print(f"Loading data from '{BOLSA_FAMILIA}'")
    df_bolsa_familia = pl.read_parquet(BOLSA_FAMILIA).with_columns(
        cpf = pl.col("cpf_favorecido").str.replace_all(
            r"\D", ""
        ),
        nome = normalize_column("nome_favorecido")
    ).select(
        [
            pl.col("cpf"),
            pl.col("nome")
        ]
    )

    print("All data loaded successfully")

    print("Initializing SQLContext")
    ctx = pl.SQLContext(
        contratos = df_contratos,
        socio = df_socio,
        bolsa_familia = df_bolsa_familia,
        eager=True
    )
    print("SQLContext initialized successfully")


    print("Executing SQL query")
    socio_bolsa = ctx.execute(
        """
            select
                c.id,
                case
                    when sum(
                        case
                            when b.cpf is null then 0
                            else 1
                        end
                    ) > 0 then true
                    else false
                end as socio_bolsa
            from
                contratos as c
            left join socio as s on
                c.cnpj_cpf = s.cnpj
            left join bolsa_familia as b on
                b.nome = s.nome and
                b.cpf = s.cnpj_cpf
            group by c.id
        """
    )
    print(socio_bolsa)
    print(f"Saving socio_bolsa data to '{SOCIO_BOLSA}'")

    socio_bolsa.write_parquet(
        SOCIO_BOLSA,
        compression="gzip",
    )
    print("socio_bolsa data saved successfully")

if __name__ == "__main__":
    print("Debuging socio_bolsa...")
    main()
    print("Debugging finished")
