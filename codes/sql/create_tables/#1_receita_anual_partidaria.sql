create or replace table main.receita_anual_partidaria as
select
	*
from read_csv(
	'/home/fontes/Documentos/repos/projeto_capivara/data/prestacao_contas_anual_partidaria/raw/receita_anual/*.csv',
	delim = ";",
	nullstr = "#NULO#",
	union_by_name=true
)