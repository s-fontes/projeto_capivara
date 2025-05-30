create
or replace table flags.socio_doador_partido as
with
    doador_partido as (
        select
            nr_cpf_cnpj_doador[4:-3] as cpf_cnpj,
            nfc_normalize (upper(strip_accents (trim(nm_doador)))) as nome,
            sum(vr_receita) as total_doado
        from
            main.receita_anual_partidaria
        group by
            nr_cpf_cnpj_doador,
            nfc_normalize (upper(strip_accents (trim(nm_doador))))
    ),
    socio as (
        select
            cnpj,
            regexp_replace(cnpj_cpf_socio, '\D', '', 'g') as cpf_cnpj,
            nfc_normalize (upper(strip_accents (trim(nome_socio)))) as nome
        from
            main.socio
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
    coalesce(dp.total_doado, 0) as total_doado,
    case
        when dp.total_doado > 0 then true
        else false
    end as socio_doador_partido
from
    contratos_compras as cc
    left join socio as s on cc.cpf_cnpj = s.cnpj
    and cc.fornecedor_tipo = 'JURIDICA'
    left join doador_partido as dp on s.cpf_cnpj = dp.cpf_cnpj
    and s.nome = dp.nome