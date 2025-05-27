create
or replace table main.empresas_punidas_cgu as
select
	*
from
	'/home/fontes/Documentos/repos/projeto_capivara/data/empresas_punidas_cgu.parquet'