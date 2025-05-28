create
or replace table main.bolsa_familia as
with
    all_data as (
        select
            *
        from
            main.bolsa_familia_pagamentos
        union all
        select
            *
        from
            main.novo_bolsa_familia
    )
select
    cpf_favorecido,
    nome_favorecido,
    sum(valor_total) as valor_total
from
    all_data
group by
    cpf_favorecido,
    nome_favorecido