create
or replace table flags.socio_contratado_candidato as
with
    contratado_candidato as (
        select
            nr_cpf_cnpj_fornecedor[4:-3] as cpf_cnpj,
            nfc_normalize (upper(strip_accents (trim(nm_fornecedor_rfb)))) as nome,
            sum(vr_despesa_contratada) as total_contratado
        from
            main.despesas_contratadas_candidatos
        group by
            nr_cpf_cnpj_fornecedor,
            nfc_normalize (upper(strip_accents (trim(nm_fornecedor_rfb))))
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
    sum(dcc.total_contratado) as total_contratado,
    case
        when sum(dcc.total_contratado) > 0 then true
        else false
    end as socio_contratado_candidato
from
    contratos_compras as cc
    left join socio as s on cc.cpf_cnpj = s.cnpj
    and cc.fornecedor_tipo = 'JURIDICA'
    left join contratado_candidato as dcc on s.cpf_cnpj = dcc.cpf_cnpj
    and s.nome = dcc.nome
group by 
    cc.id