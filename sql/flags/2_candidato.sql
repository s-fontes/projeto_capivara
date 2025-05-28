create
or replace table flags.candidato as
with
    despesas_candidatos as (
        select
            nr_cpf_candidato as cpf,
            sum(vr_despesa_contratada) as total_
        from
            main.despesas_contratadas_candidatos
        group by
            nr_cpf_candidato
    ),
    receitas_candidatos as (
        select
            nr_cpf_candidato as cpf,
            sum(vr_receita) as total_receita
        from
            main.receitas_candidatos
        group by
            nr_cpf_candidato
    ),
    candidatos as (
        select
            cpf,
            coalesce(total_, 0) as total_,
            coalesce(total_receita, 0) as total_receita
        from
            despesas_candidatos
            full outer join receitas_candidatos using (cpf)
    ),
    contratos_compras as (
        select
            id,
            regexp_replace(fornecedor_cnpj_cpf_idgener, '\D', '', 'g') as cpf,
            fornecedor_tipo
        from
            main.contratos_compras
    )
select
    cc.id,
    coalesce(c.total_, 0) as total_,
    coalesce(c.total_receita, 0) as total_receita,
    case
        when c.total_ > 0
        or c.total_receita > 0 then true
        else false
    end as candidato
from
    contratos_compras as cc
    left join candidatos as c on cc.cpf = c.cpf
    and cc.fornecedor_tipo = 'FISICA'