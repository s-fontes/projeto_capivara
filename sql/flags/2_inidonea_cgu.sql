create
or replace table flags.inidonea_cgu as
with
    inidonea_cgu as (
        select
            "CPF OU CNPJ DO SANCIONADO" as cpf_cnpj,
            count(*) as total_sancoes
        from
            main.empresa_inidonea_cgu
        group by
            "CPF OU CNPJ DO SANCIONADO"
    ),
    contratos_compras as (
        select
            id,
            regexp_replace(fornecedor_cnpj_cpf_idgener, '\D', '', 'g') as cpf_cnpj,
            fornecedor_tipo
        from
            main.contratos_compras
    )
select
    cc.id,
    coalesce(icgu.total_sancoes, 0) as total_sancoes,
    case
        when icgu.total_sancoes > 0 then true
        else false
    end as inidonea_cgu
from
    contratos_compras as cc
    left join inidonea_cgu as icgu on cc.cpf_cnpj = cast(icgu.cpf_cnpj as varchar)