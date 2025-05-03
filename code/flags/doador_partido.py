import os
import polars as pl

import sys
sys.path.append(os.path.abspath(__file__ + "/../../.."))

from common import get_path

CONTRATOS = get_path("/data/contratos/bronze/contratos.parquet")
RECEITA = get_path("/data/prestacao_contas_anual_partidaria/bronze/receitas.parquet")
DOADOR_PARTIDO = get_path("/data/flags/doador_partido.parquet")

def main():

    print("Starting doador_partido data process...")

    if not os.path.isdir(os.path.dirname(DOADOR_PARTIDO)):
        os.makedirs(os.path.dirname(DOADOR_PARTIDO))

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

    print(f"Loading data from '{RECEITA}'")
    df_receita = pl.read_parquet(RECEITA).with_columns(
        cnpj_cpf=pl.col("NR_CPF_CNPJ_DOADOR").str.replace_all(
            r"\D", ""
        ).str.pad_start(
            11, "0"
        )
    ).select(
        [
            pl.col("cnpj_cpf")
        ]
    )

    print("All data loaded successfully")

    print("Initializing SQLContext")
    ctx = pl.SQLContext(
        contratos=df_contratos,
        receita=df_receita,
        eager=True
    )
    print("SQLContext initialized successfully")

    print("Executing SQL query")
    doador_partido = ctx.execute(
        """
            select
                c.id,
                case
                    when sum(
                        case
                            when r.cnpj_cpf is null then 0
                            else 1
                        end
                    ) > 0 then true
                    else false
                end as doador_partido
            from
                contratos as c
            left join receita as r on c.cnpj_cpf = r.cnpj_cpf
            group by c.id
        """
    )
    print("SQL query executed successfully")
    print(doador_partido)
    print(f"Saving doador_partido to '{DOADOR_PARTIDO}'")
    doador_partido.write_parquet(
        DOADOR_PARTIDO,
        compression="gzip"
    )
    print("doador_partido saved successfully")


if __name__ == "__main__":
    print("Debuging doador_partido...")
    main()
    print("Debugging finished")
