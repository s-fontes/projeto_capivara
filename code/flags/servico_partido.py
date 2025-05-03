import os
import polars as pl

import sys
sys.path.append(os.path.abspath(__file__ + "/../../.."))

from common import get_path

CONTRATOS = get_path("/data/contratos/bronze/contratos.parquet")
DESPESA = get_path("/data/prestacao_contas_anual_partidaria/bronze/despesas.parquet")
SERVICO_PARTIDO = get_path("/data/flags/servico_partido.parquet")

def main():

    print("Starting servico_partido data process...")

    if not os.path.isdir(os.path.dirname(SERVICO_PARTIDO)):
        os.makedirs(os.path.dirname(SERVICO_PARTIDO))

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

    print(f"Loading data from '{DESPESA}'")
    df_despesa = pl.read_parquet(DESPESA).with_columns(
        cnpj_cpf=pl.col("NR_CPF_CNPJ_FORNECEDOR")
    ).select(
        [
            pl.col("cnpj_cpf")
        ]
    )

    print("All data loaded successfully")

    print("Initializing SQLContext")
    ctx = pl.SQLContext(
        contratos=df_contratos,
        despesa=df_despesa,
        eager=True
    )
    print("SQLContext initialized successfully")

    print("Executing SQL query")
    servico_partido = ctx.execute(
        """
            select
                c.id,
                case
                    when sum(
                        case
                            when d.cnpj_cpf is null then 0
                            else 1
                        end
                    ) > 0 then true
                    else false
                end as servico_partido
            from
                contratos as c
            left join despesa as d on c.cnpj_cpf = d.cnpj_cpf
            group by c.id
        """
    )
    print("SQL query executed successfully")
    print(servico_partido)
    print(f"Saving servico_partido to '{SERVICO_PARTIDO}'")
    servico_partido.write_parquet(
        SERVICO_PARTIDO,
        compression="gzip"
    )
    print("servico_partido saved successfully")


if __name__ == "__main__":
    print("Debuging servico_partido...")
    main()
    print("Debugging finished")
