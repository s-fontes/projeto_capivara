create
or replace table main.candidatos as
select
	*
from
	read_csv (
		'/home/fontes/Documentos/repos/projeto_capivara/data/candidatos/raw/votacao_candidato_munzona/*.csv',
		delim = ";",
		nullstr = ["#NULO#", "#NULO"],
		union_by_name = true,
		null_padding = true,
		normalize_names = true
	)