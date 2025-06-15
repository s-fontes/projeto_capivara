create
or replace table main.empresas_inidoneas_tcu as
select
	*
from
	'/home/fontes/Documentos/repos/projeto_capivara/data/empresas_inidoneas_tcu.parquet'