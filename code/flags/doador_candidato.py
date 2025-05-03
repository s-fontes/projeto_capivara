import os
import polars as pl

import sys
sys.path.append(os.path.abspath(__file__ + "/../../.."))

from common import get_path

CONTRATOS = get_path("/data/contratos/bronze/contratos.parquet")
DOADOR_ORIGINARIO = get_path("/data/prestacao_de_contas_eleitorais_candidatos/bronze/receitas_doador_originario")
DOADOR_CANDIDATO = get_path("/data/flags/doador_candidato.parquet")

def main():

    print("Starting doador_candidato data process...")

    if not os.path.isdir(os.path.dirname(DOADOR_CANDIDATO)):
        os.makedirs(os.path.dirname(DOADOR_CANDIDATO))

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

    print(f"Loading data from '{DOADOR_ORIGINARIO}'")
    df_doador_originario = pl.read_parquet(DOADOR_ORIGINARIO).with_columns(
        cnpj_cpf=pl.col("NR_CPF_CNPJ_DOADOR_ORIGINARIO").str.replace_all(
            r"\D", ""
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
        doador_originario=df_doador_originario,
        eager=True
    )
    print("SQLContext initialized successfully")

    print("Executing SQL query")
    doador_candidato = ctx.execute(
        """
            select
                c.id,
                case
                    when sum(
                        case
                            when do.cnpj_cpf is null then 0
                            else 1
                        end
                    ) > 0 then true
                    else false
                end as doador_candidato
            from
                contratos as c
            left join doador_originario as do on c.cnpj_cpf = do.cnpj_cpf
            group by c.id
        """
    )
    print("SQL query executed successfully")
    print(doador_candidato)
    print(f"Saving doador_candidato to '{DOADOR_CANDIDATO}'")
    doador_candidato.write_parquet(
        DOADOR_CANDIDATO,
        compression="gzip"
    )
    print("doador_candidato saved successfully")


if __name__ == "__main__":
    print("Debuging doador_candidato...")
    main()
    print("Debugging finished")
