create or replace table main.receitas_candidatos_doador_originario as
select
	*
from read_csv(
	'/home/fontes/Documentos/repos/projeto_capivara/data/prestacao_de_contas_eleitorais_candidatos/raw/receitas_candidatos_doador_originario/*.csv',
	delim = ";",
	nullstr = "#NULO",
	union_by_name=true
)