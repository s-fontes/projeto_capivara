create
or replace table flags.contratado_partido as
with
    contratado_partido as (
        select
            nr_cpf_cnpj_fornecedor as cpf_cnpj,
            sum(cast(vr_pagamento as decimal)) as total_pago
        from
            main.despesa_anual_partidaria
        group by
            nr_cpf_cnpj_fornecedor
    ),
    contratos_compras as (
        select
            id,
            regexp_replace (fornecedor_cnpj_cpf_idgener, '\D', '', 'g') as cpf_cnpj
        from
            main.contratos_compras
    )
select
    cc.id,
    coalesce(dcc.total_pago, 0) as total_pago,
    case
        when dcc.total_pago > 0 then true
        else false
    end as contratado_partido
from
    contratos_compras as cc
    left join contratado_partido as dcc on cc.cpf_cnpj = dcc.cpf_cnpj