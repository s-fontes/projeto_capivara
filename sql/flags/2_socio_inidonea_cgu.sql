create
or replace table flags.socio_inidonea_cgu as
with
    inidonea_cgu as (
        select
            "CPF OU CNPJ DO SANCIONADO" as cpf_cnpj,
            nfc_normalize (upper(strip_accents (trim("NOME DO SANCIONADO")))) as nome,
            count(*) as total_sancoes
        from
            main.empresa_inidonea_cgu
        where
            "TIPO DE PESSOA" = 'F'
        group by
            "CPF OU CNPJ DO SANCIONADO",
            nfc_normalize (upper(strip_accents (trim("NOME DO SANCIONADO"))))
    ),
    socio as (
        select
            cnpj,
            regexp_replace (cnpj_cpf_socio, '\D', '', 'g') as cpf_cnpj,
            nfc_normalize (upper(strip_accents (trim(nome_socio)))) as nome
        from
            main.socio
    ),
    contratos_compras as (
        select
            id,
            regexp_replace (fornecedor_cnpj_cpf_idgener, '\D', '', 'g') as cpf_cnpj,
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
    end as socio_inidonea_cgu
from
    contratos_compras as cc
    left join socio as s on cc.cpf_cnpj = s.cnpj
    and cc.fornecedor_tipo = 'JURIDICA'
    left join inidonea_cgu as icgu on s.cpf_cnpj = cast(icgu.cpf_cnpj as varchar)
    and s.nome = icgu.nome