create
or replace table flags.contratado_candidato as
with
    contratado_candidato as (
        select
            nr_cpf_cnpj_fornecedor as cpf_cnpj,
            sum(vr_despesa_contratada) as total_contratado
        from
            main.despesas_contratadas_candidatos
        group by
            nr_cpf_cnpj_fornecedor
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
    coalesce(dcc.total_contratado, 0) as total_contratado,
    case
        when dcc.total_contratado > 0 then true
        else false
    end as contratado_candidato
from
    contratos_compras as cc
    left join contratado_candidato as dcc on cc.cpf_cnpj = dcc.cpf_cnpj