import os
import polars as pl

import sys
sys.path.append(os.path.abspath(__file__ + "/../../.."))

from common import get_path

CONTRATOS = get_path("/data/contratos/bronze/contratos.parquet")
EMPRESA_INIDONEA_TCU = get_path("/data/other/empresa_inidonea_tcu.parquet")
INIDONEA_TCU = get_path("/data/flags/inidonea_tcu.parquet")

def main():

    print("Starting inidonea_tcu data process...")

    if not os.path.isdir(os.path.dirname(INIDONEA_TCU)):
        os.makedirs(os.path.dirname(INIDONEA_TCU))

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

    print(f"Loading data from '{EMPRESA_INIDONEA_TCU}'")
    df_empresa_inidonea_tcu = pl.read_parquet(EMPRESA_INIDONEA_TCU).with_columns(
        cnpj_cpf=pl.col("cpf_cnpj").str.replace_all(
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
        empresa_inidonea_tcu=df_empresa_inidonea_tcu,
        eager=True
    )
    print("SQLContext initialized successfully")

    print("Executing SQL query")
    inidonea_tcu = ctx.execute(
        """
            select
                c.id,
                case
                    when sum(
                        case
                            when e.cnpj_cpf is null then 0
                            else 1
                        end
                    ) > 0 then true
                    else false
                end as inidonea_tcu
            from
                contratos as c
            left join empresa_inidonea_tcu as e on c.cnpj_cpf = e.cnpj_cpf
            group by c.id
        """
    )
    print("SQL query executed successfully")
    print(inidonea_tcu)
    print(f"Saving inidonea_tcu to '{INIDONEA_TCU}'")
    inidonea_tcu.write_parquet(
        INIDONEA_TCU,
        compression="gzip"
    )
    print("inidonea_tcu saved successfully")


if __name__ == "__main__":
    print("Debuging inidonea_tcu...")
    main()
    print("Debugging finished")
