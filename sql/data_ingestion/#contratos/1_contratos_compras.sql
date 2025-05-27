create
or replace table main.contratos_comptras as
select
	*
from
	read_json(
		'/home/fontes/Documentos/repos/projeto_capivara/data/contratos_compras/*.json',
		union_by_name = true
	)