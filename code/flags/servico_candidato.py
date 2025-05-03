import os
import polars as pl

import sys
sys.path.append(os.path.abspath(__file__ + "/../../.."))

from common import get_path

CONTRATOS = get_path("/data/contratos/bronze/contratos.parquet")
DESPESAS_CONTRATADAS = get_path("/data/prestacao_de_contas_eleitorais_candidatos/bronze/despesas_contratadas")
SERVICO_CANDIDATO = get_path("/data/flags/servico_candidato.parquet")

def main():

    print("Starting servico_candidato data process...")

    if not os.path.isdir(os.path.dirname(SERVICO_CANDIDATO)):
        os.makedirs(os.path.dirname(SERVICO_CANDIDATO))

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

    print(f"Loading data from '{DESPESAS_CONTRATADAS}'")
    df_despesas_contratadas = pl.read_parquet(DESPESAS_CONTRATADAS).with_columns(
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
        despesas_contratadas=df_despesas_contratadas,
        eager=True
    )
    print("SQLContext initialized successfully")

    print("Executing SQL query")
    servico_candidato = ctx.execute(
        """
            select
                c.id,
                case
                    when sum(
                        case
                            when dc.cnpj_cpf is null then 0
                            else 1
                        end
                    ) > 0 then true
                    else false
                end as servico_candidato
            from
                contratos as c
            left join despesas_contratadas as dc on c.cnpj_cpf = dc.cnpj_cpf
            group by c.id
        """
    )
    print("SQL query executed successfully")
    print(servico_candidato)
    print(f"Saving servico_candidato to '{SERVICO_CANDIDATO}'")
    servico_candidato.write_parquet(
        SERVICO_CANDIDATO,
        compression="gzip"
    )
    print("servico_candidato saved successfully")


if __name__ == "__main__":
    print("Debuging servico_candidato...")
    main()
    print("Debugging finished")
