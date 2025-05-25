create table main.bolsa_familia_agg as
select
    cpf_favorecido,
    nome_favorecido,
    sum(valor_parcela) as total_pago
from main.bolsa_familia
group by cpf_favorecido, nome_favorecido