create
or replace table main.empresa as
select
	*
from
	read_csv(
		"./data/empresas/empresas/*/*",
		delim = ";",
		header = false,
		columns = {
			'cnpj': 'VARCHAR',
			'razao_social': 'VARCHAR',
			'natureza': 'INTEGER',
			'qualificacao': 'INTEGER',
			'capital_social': 'DECIMAL(20,2)',
			'porte_da_empresa': 'VARCHAR',
			'ente_federativo_responsavel': 'VARCHAR',
		}
	)