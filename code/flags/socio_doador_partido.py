import os
import polars as pl

import sys
sys.path.append(os.path.abspath(__file__ + "/../../.."))

from common import get_path, normalize_column

CONTRATOS = get_path("/data/contratos/bronze/contratos.parquet")
SOCIO = get_path("/data/other/socio.parquet")
RECEITA = get_path("/data/prestacao_contas_anual_partidaria/bronze/receitas.parquet")
SOCIO_DOADOR_PARTIDO = get_path("/data/flags/socio_doador_partido.parquet")


def main():

    print("Starting socio_politico data process...")

    if not os.path.isdir(os.path.dirname(SOCIO_DOADOR_PARTIDO)):
        os.makedirs(os.path.dirname(SOCIO_DOADOR_PARTIDO))

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

    print(f"Loading data from '{RECEITA}'")
    df_receita = pl.read_parquet(RECEITA).with_columns(
        cnpj_cpf=pl.col("NR_CPF_CNPJ_DOADOR").str.pad_start(
            11, "0"
        ).str.slice(4, 7),
        nome=normalize_column("NM_DOADOR")
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
        receita=df_receita,
        eager=True
    )
    print("SQLContext initialized successfully")

    print("Executing SQL query")
    socio_doador_partido = ctx.execute(
        """
            select
                c.id,
                case
                    when sum(
                        case
                            when r.nome is null then 0
                            else 1
                        end
                    ) > 0 then true
                    else false
                end as socio_doador_partido
            from
                contratos as c
            left join socio as s on
                c.cnpj_cpf = s.cnpj
            left join receita as r on
                r.nome = s.nome and
                r.cnpj_cpf = s.cnpj_cpf
            group by c.id
        """
    )
    print(socio_doador_partido)
    print(f"Saving socio_doador_partido to '{SOCIO_DOADOR_PARTIDO}'")
    socio_doador_partido.write_parquet(
        SOCIO_DOADOR_PARTIDO,
        compression="gzip"
    )
    print("socio_doador_partido saved successfully")


if __name__ == "__main__":
    print("Debuging socio_politico...")
    main()
    print("Debugging finished")
