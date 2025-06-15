create
or replace table flags.punida_cgu as
with
    punida_cgu as (
        select
            "CPF OU CNPJ DO SANCIONADO" as cpf_cnpj,
            count(*) as total_sancoes
        from
            main.empresas_punidas_cgu
        group by
            "CPF OU CNPJ DO SANCIONADO"
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
    coalesce(pcgu.total_sancoes, 0) as total_sancoes,
    case
        when pcgu.total_sancoes > 0 then true
        else false
    end as punida_cgu
from
    contratos_compras as cc
    left join punida_cgu as pcgu on cc.cpf_cnpj = cast(pcgu.cpf_cnpj as varchar)