import os
import polars as pl

import sys
sys.path.append(os.path.abspath(__file__ + "/../../.."))

from common import get_path

CONTRATOS = get_path("/data/contratos/bronze/contratos.parquet")
EMPRESA_INIDONEA_CGU = get_path("/data/other/empresa_inidonea_cgu.parquet")
INIDONEA_CGU = get_path("/data/flags/inidonea_cgu.parquet")

def main():

    print("Starting inidonea_cgu data process...")

    if not os.path.isdir(os.path.dirname(INIDONEA_CGU)):
        os.makedirs(os.path.dirname(INIDONEA_CGU))

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

    print(f"Loading data from '{EMPRESA_INIDONEA_CGU}'")
    df_empresa_inidonea_cgu = pl.read_parquet(EMPRESA_INIDONEA_CGU).rename(
        lambda col_name: col_name.replace(' ', '_')
    ).with_columns(
        cnpj_cpf=pl.when(
            pl.col("TIPO_DE_PESSOA").eq("F")
        ).then(
            pl.col("CPF_OU_CNPJ_DO_SANCIONADO").cast(pl.String()).str.pad_start(11, "0")
        ).otherwise(
            pl.col("CPF_OU_CNPJ_DO_SANCIONADO").cast(pl.String()).str.pad_start(14, "0")
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
        empresa_inidonea_cgu=df_empresa_inidonea_cgu,
        eager=True
    )
    print("SQLContext initialized successfully")

    print("Executing SQL query")
    inidonea_cgu = ctx.execute(
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
                end as inidonea_cgu
            from
                contratos as c
            left join empresa_inidonea_cgu as e on c.cnpj_cpf = e.cnpj_cpf
            group by c.id
        """
    )
    print("SQL query executed successfully")
    print(inidonea_cgu)
    print(f"Saving inidonea_cgu to '{INIDONEA_CGU}'")
    inidonea_cgu.write_parquet(
        INIDONEA_CGU,
        compression="gzip"
    )
    print("inidonea_cgu saved successfully")


if __name__ == "__main__":
    print("Debuging inidonea_cgu...")
    main()
    print("Debugging finished")
