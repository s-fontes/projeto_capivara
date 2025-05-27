create
or replace table main.despesa_anual_partidaria as
select
	*
from
	read_csv (
		'/home/fontes/Documentos/repos/projeto_capivara/data/prestacao_contas_anual_partidaria/raw/despesa_anual/*.csv',
		delim = ";",
		nullstr = "#NULO#",
		union_by_name = true,
		null_padding = true,
		normalize_names = true
	)