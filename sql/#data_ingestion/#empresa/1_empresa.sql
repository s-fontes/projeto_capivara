create
or replace table main.empresa as
select
	*
from
	'./data/empresa.parquet'