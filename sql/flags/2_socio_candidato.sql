create
or replace table flags.socio_candidato as
with
    despesas_candidatos as (
        select
            nr_cpf_candidato[4:-3] as cpf,
            nfc_normalize (upper(strip_accents (trim(nm_candidato)))) as nome,
            sum(vr_despesa_contratada) as total_despesa
        from
            main.despesas_contratadas_candidatos
        group by
            nr_cpf_candidato,
            nfc_normalize (upper(strip_accents (trim(nm_candidato))))
    ),
    receitas_candidatos as (
        select
            nr_cpf_candidato[4:-3] as cpf,
            nfc_normalize (upper(strip_accents (trim(nm_candidato)))) as nome,
            sum(vr_receita) as total_receita
        from
            main.receitas_candidatos
        group by
            nr_cpf_candidato,
            nfc_normalize (upper(strip_accents (trim(nm_candidato))))
    ),
    candidatos as (
        select
            cpf,
            nome,
            coalesce(total_despesa, 0) as total_despesa,
            coalesce(total_receita, 0) as total_receita
        from
            despesas_candidatos
            full outer join receitas_candidatos using (cpf, nome)
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
            regexp_replace(fornecedor_cnpj_cpf_idgener, '\D', '', 'g') as cpf,
            fornecedor_tipo
        from
            main.contratos_compras
    )
select
    cc.id,
    coalesce(c.total_despesa, 0) as total_despesa,
    coalesce(c.total_receita, 0) as total_receita,
    c.nome,
    case
        when c.total_despesa > 0
        or c.total_receita > 0 then true
        else false
    end as socio_candidato
from
    contratos_compras as cc
    left join socio as s on cc.cpf = s.cpf_cnpj
    and cc.fornecedor_tipo = 'JURIDICA'
    left join candidatos as c on s.cpf_cnpj = c.cpf
    and s.nome = c.nome