create or replace table main.novo_bolsa_familia as
select
    cpf_favorecido,
    nome_favorecido,
    sum(valor_parcela) as valor_total
from
    read_csv (
        '/home/fontes/Documentos/repos/projeto_capivara/data/bolsa_familia/raw/NovoBolsaFamilia/*.csv',
        delim = ';',
        header = true,
        union_by_name = true,
        null_padding = true,
        normalize_names = true
    )
where
    regexp_full_match (cpf_favorecido, '^\*{3}\.\d{3}\.\d{3}-\*{2}$')
    and regexp_full_match (nome_favorecido, '^\D*$')
    and valor_parcela > 0
group by
    cpf_favorecido,
    nome_favorecido