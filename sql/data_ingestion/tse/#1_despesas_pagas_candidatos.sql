create
or replace table main.despesas_pagas_candidatos as
select
	*
from
	read_csv (
		'/home/fontes/Documentos/repos/projeto_capivara/data/prestacao_de_contas_eleitorais_candidatos/raw/despesas_pagas_candidatos/*.csv',
		delim = ";",
		nullstr = "#NULO#",
		union_by_name = true,
		null_padding = true,
		normalize_names = true
	)