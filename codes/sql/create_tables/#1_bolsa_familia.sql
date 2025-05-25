create or replace table main.bolsa_familia as
with all_data as (
    select
        cpf_favorecido,
        nome_favorecido,
        valor_parcela,
        mes_referencia
    from read_csv(
        '/home/fontes/Documentos/repos/projeto_capivara/data/bolsa_familia/raw/NovoBolsaFamilia/*.csv',
        delim = ';',
        header = true,
        union_by_name = true,
        null_padding = true,
        normalize_names = true
        -- parallel = false
    )
    union all
    select
        cpf_favorecido,
        nome_favorecido,
        valor_parcela,
        mes_referencia
    from read_csv(
        '/home/fontes/Documentos/repos/projeto_capivara/data/bolsa_familia/raw/BolsaFamilia_Pagamentos/*.csv',
        delim = ';',
        header = true,
        union_by_name = true,
        null_padding = true,
        normalize_names = true
        -- parallel = false
    )
)
select
    cpf_favorecido,
    nome_favorecido,
    valor_parcela,
    mes_referencia
from all_data
where  regexp_full_match(cpf_favorecido, '^\*{3}\.\d{3}\.\d{3}-\*{2}$')
and regexp_full_match(nome_favorecido, '^\D*$')
and valor_parcela > 0
and mes_referencia is not null