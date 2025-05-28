create
or replace table flags.doador_originario_candidato as
with
    doador_originario_candidato as (
        select
            nr_cpf_cnpj_doador_originario as cpf_cnpj,
            sum(vr_receita) as total_doado
        from
            main.receitas_candidatos_doador_originario
        group by
            nr_cpf_cnpj_doador_originario
    ),
    contratos_compras as (
        select
            id,
            regexp_replace (fornecedor_cnpj_cpf_idgener, '\D', '', 'g') as cpf_cnpj
        from
            main.contratos_compras
    )
select
    cc.id,
    coalesce(dc.total_doado, 0) as total_doado,
    case
        when dc.total_doado > 0 then true
        else false
    end as doador_originario_candidato
from
    contratos_compras as cc
    left join doador_originario_candidato as dc on cc.cpf_cnpj = dc.cpf_cnpj