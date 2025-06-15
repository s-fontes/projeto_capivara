create
or replace table main.contratos_compras as
select
	*
from
	read_json(
		'/home/fontes/Documentos/repos/projeto_capivara/data/contratos_compras/*.json',
		union_by_name = true
	)