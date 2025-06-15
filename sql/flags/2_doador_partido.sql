create
or replace table flags.doador_partido as
with
    doador_partido as (
        select
            nr_cpf_cnpj_doador as cpf_cnpj,
            sum(vr_receita) as total_doado
        from
            main.receita_anual_partidaria
        group by
            nr_cpf_cnpj_doador
    ),
    contratos_compras as (
        select
            id,
            regexp_replace(fornecedor_cnpj_cpf_idgener, '\D', '', 'g') as cpf_cnpj
        from
            main.contratos_compras
    )
select
    cc.id,
    coalesce(dcc.total_doado, 0) as total_doado,
    case
        when dcc.total_doado > 0 then true
        else false
    end as doador_partido
from
    contratos_compras as cc
    left join doador_partido as dcc on cc.cpf_cnpj = dcc.cpf_cnpj