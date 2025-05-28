create
or replace table flags.bolsa_familia as
with
    bolsa_familia as (
        select
            regexp_replace(cpf_favorecido, '\D', '', 'g') as cpf,
            nfc_normalize (upper(strip_accents (trim(nome_favorecido)))) as nome,
            valor_total
        from
            main.bolsa_familia
    ),
    contratos_compras as (
        select
            id,
            regexp_replace(fornecedor_cnpj_cpf_idgener, '\D', '', 'g') [4:-3] as cpf,
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
    end as bolsa_familia
from
    contratos_compras as cc
    left join bolsa_familia as bf on cc.cpf = bf.cpf
    and cc.nome = bf.nome
    and cc.fornecedor_tipo = 'FISICA'