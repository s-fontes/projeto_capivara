create
or replace table flags.socio_punida_cgu as
with
    punida_cgu as (
        select
            "CPF OU CNPJ DO SANCIONADO" as cpf_cnpj,
            nfc_normalize (upper(strip_accents (trim("NOME DO SANCIONADO")))) as nome,
            count(*) as total_sancoes
        from
            main.empresas_punidas_cgu
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
    sum(pcgu.total_sancoes) as total_sancoes,
    case
        when sum(pcgu.total_sancoes) > 0 then true
        else false
    end as socio_punida_cgu
from
    contratos_compras as cc
    left join socio as s on cc.cpf_cnpj = s.cnpj
    and cc.fornecedor_tipo = 'JURIDICA'
    left join punida_cgu as pcgu on s.cpf_cnpj = cast(pcgu.cpf_cnpj as varchar)
    and s.nome = pcgu.nome
group by
    cc. id