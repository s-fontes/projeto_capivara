import os
import polars as pl

import sys
sys.path.append(os.path.abspath(__file__ + "/../../.."))

from common import get_path, normalize_column

CONTRATOS = get_path("/data/contratos/bronze/contratos.parquet")
EMPRESA = get_path("/data/other/empresa.parquet")
ESTABELECIMENTO = get_path("/data/other/estabelecimento.parquet")

DOADOR_CANDIDATO = get_path("/data/flags/doador_candidato.parquet")
DOADOR_PARTIDO = get_path("/data/flags/doador_partido.parquet")
INIDONEA_CGU = get_path("/data/flags/inidonea_cgu.parquet")
INIDONEA_TCU = get_path("/data/flags/inidonea_tcu.parquet")
PUNIDA_CGU = get_path("/data/flags/punida_cgu.parquet")
SERVICO_CANDIDATO = get_path("/data/flags/servico_candidato.parquet")
SERVICO_PARTIDO = get_path("/data/flags/servico_partido.parquet")
SOCIO_BOLSA = get_path("/data/flags/socio_bolsa.parquet")
SOCIO_DOADOR_CANDIDATO = get_path("/data/flags/socio_doador_candidato.parquet")
SOCIO_DOADOR_PARTIDO = get_path("/data/flags/socio_doador_partido.parquet")
SOCIO_POLITICO = get_path("/data/flags/socio_politico.parquet")
DATASET = get_path("/data/flags/dataset.parquet")


def main():

    print("Starting dataset data process...")

    if not os.path.isdir(os.path.dirname(DATASET)):
        os.makedirs(os.path.dirname(DATASET))

    print(f"Loading data from '{CONTRATOS}'")
    df_contratos = pl.read_parquet(CONTRATOS).with_columns(
        cnpj_cpf=pl.col("fornecedor_cnpj_cpf_idgener").str.replace_all(
            r"\D", ""
        )
    ).select(
        [
            pl.col("id").cast(pl.Int32),
            pl.col("vigencia_inicio"),
            pl.col("vigencia_fim"),
            pl.col("valor_global"),
            pl.col("cnpj_cpf")
        ]
    )

    print(f"Load datafrom {EMPRESA}")
    df_empresa = pl.read_parquet(EMPRESA).select(
        [
            pl.col("cnpj"),
            pl.col("capital_social")
        ]
    )

    print(f"Load data from {ESTABELECIMENTO}")
    df_estabelecimento = pl.read_parquet(ESTABELECIMENTO).select(
        [
            pl.col("cnpj"),
            pl.col("data_inicio_atividade"),
            pl.col("cnae_fiscal_princ"),
            pl.col("cnae_fiscal_sec")
        ]
    )
    print(f"Load data from {DOADOR_CANDIDATO}")
    df_doador_candidato = pl.read_parquet(DOADOR_CANDIDATO)

    print(f"Load data from {DOADOR_PARTIDO}")
    df_doador_partido = pl.read_parquet(DOADOR_PARTIDO)

    print(f"Load data from {INIDONEA_CGU}")
    df_inidonea_cgu = pl.read_parquet(INIDONEA_CGU)

    print(f"Load data from {INIDONEA_TCU}")
    df_inidonea_tcu = pl.read_parquet(INIDONEA_TCU)

    print(f"Load data from {PUNIDA_CGU}")
    df_punida_cgu = pl.read_parquet(PUNIDA_CGU)

    print(f"Load data from {SERVICO_CANDIDATO}")
    df_servico_candidato = pl.read_parquet(SERVICO_CANDIDATO)

    print(f"Load data from {SERVICO_PARTIDO}")
    df_servico_partido = pl.read_parquet(SERVICO_PARTIDO)

    print(f"Load data from {SOCIO_BOLSA}")
    df_socio_bolsa = pl.read_parquet(SOCIO_BOLSA)

    print(f"Load data from {SOCIO_DOADOR_CANDIDATO}")
    df_socio_doador_candidato = pl.read_parquet(SOCIO_DOADOR_CANDIDATO)

    print(f"Load data from {SOCIO_DOADOR_PARTIDO}")
    df_socio_doador_partido = pl.read_parquet(SOCIO_DOADOR_PARTIDO)

    print(f"Load data from {SOCIO_POLITICO}")
    df_socio_politico = pl.read_parquet(SOCIO_POLITICO)

    print("All data loaded successfully")

    print("Initializing SQLContext")
    ctx = pl.SQLContext(
        contratos=df_contratos,
        empresa=df_empresa,
        estabelecimento=df_estabelecimento,
        doador_candidato=df_doador_candidato,
        doador_partido=df_doador_partido,
        inidonea_cgu=df_inidonea_cgu,
        inidonea_tcu=df_inidonea_tcu,
        punida_cgu=df_punida_cgu,
        servico_candidato=df_servico_candidato,
        servico_partido=df_servico_partido,
        socio_bolsa=df_socio_bolsa,
        socio_doador_candidato=df_socio_doador_candidato,
        socio_doador_partido=df_socio_doador_partido,
        socio_politico=df_socio_politico,
        eager=True
    )
    print("SQLContext initialized successfully")

    print("Executing SQL query")
    dataset = ctx.execute(
        """
            select
                c.id,
                c.vigencia_inicio,
                c.vigencia_fim,
                c.valor_global,
                c.cnpj_cpf,
                e.capital_social,
                est.data_inicio_atividade,
                est.cnae_fiscal_princ,
                est.cnae_fiscal_sec,
                dc.doador_candidato,
                dp.doador_partido,
                ic.inidonea_cgu,
                it.inidonea_tcu,
                pc.punida_cgu,
                sc.servico_candidato,
                sp.servico_partido,
                sb.socio_bolsa,
                sdc.socio_doador_candidato,
                sdp.socio_doador_partido,
                spo.socio_politico
            from
                contratos as c
            left join empresa as e on
                c.cnpj_cpf = e.cnpj
            left join estabelecimento as est on
                c.cnpj_cpf = est.cnpj
            left join doador_candidato as dc on
                c.id = dc.id
            left join doador_partido as dp on
                c.id = dp.id
            left join inidonea_cgu as ic on
                c.id = ic.id
            left join inidonea_tcu as it on
                c.id = it.id
            left join punida_cgu as pc on
                c.id = pc.id
            left join servico_candidato as sc on
                c.id = sc.id
            left join servico_partido as sp on
                c.id = sp.id
            left join socio_bolsa as sb on
                c.id = sb.id
            left join socio_doador_candidato as sdc on
                c.id = sdc.id
            left join socio_doador_partido as sdp on
                c.id = sdp.id
            left join socio_politico as spo on
                c.id = spo.id
        """
    )
    print("SQL query executed successfully")
    print(dataset)
    print(f"Saving dataset to '{DATASET}'")
    dataset.write_parquet(
        DATASET,
        compression="gzip"
    )
    print("dataset saved successfully")


if __name__ == "__main__":
    print("Debuging dataset...")
    main()
    print("Debugging finished")
