create
or replace table flags.inidonea_tcu as
with
    inidonea_tcu as (
        select
            regexp_replace(cpf_cnpj, '\D', '', 'g') as cpf_cnpj,
            count(*) as total_sancoes
        from
            main.empresas_inidoneas_tcu
        group by
            cpf_cnpj
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
    coalesce(itcu.total_sancoes, 0) as total_sancoes,
    case
        when itcu.total_sancoes > 0 then true
        else false
    end as inidonea_tcu
from
    contratos_compras as cc
    left join inidonea_tcu as itcu on cc.cpf_cnpj = cast(itcu.cpf_cnpj as varchar)