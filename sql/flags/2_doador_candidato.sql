create
or replace table flags.doador_candidato as
with
    doador_candidato as (
        select
            nr_cpf_cnpj_doador as cpf_cnpj,
            sum(vr_receita) as total_doado
        from
            main.receitas_candidatos
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
    coalesce(dc.total_doado, 0) as total_doado,
    case
        when dc.total_doado > 0 then true
        else false
    end as doador_candidato
from
    contratos_compras as cc
    left join doador_candidato as dc on cc.cpf_cnpj = dc.cpf_cnpj