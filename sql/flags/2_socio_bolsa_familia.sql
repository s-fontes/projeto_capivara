-- create
-- or replace table flags.socio_bolsa_familia as
with
    bolsa_familia as (
        select
            regexp_replace(cpf_favorecido, '\D', '', 'g') as cpf,
            nfc_normalize (upper(strip_accents (trim(nome_favorecido)))) as nome,
            valor_total
        from
            main.bolsa_familia
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
            nfc_normalize (upper(strip_accents (trim(fornecedor_nome)))) as nome,
            fornecedor_tipo
        from
            main.contratos_compras
    )
select
    cc.id,
    coalesce(bf.valor_total, 0) as valor_total,
    case
        when bf.valor_total > 0 then true
        else false
    end as socio_bolsa_familia
from
    contratos_compras as cc
    left join socio as s on cc.cpf_cnpj = s.cnpj
    and cc.fornecedor_tipo = 'JURIDICA'
    left join bolsa_familia as bf on s.cpf_cnpj = bf.cpf
    and s.nome = bf.nome