create
or replace table main.contratos_pncp as
select
	*
from
	read_json(
		'/home/fontes/Documentos/repos/projeto_capivara/data/contratos_pncp/*.json',
		union_by_name = true
	)