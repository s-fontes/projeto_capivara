create
or replace table main.estabelecimento as
select
	*
from
	"./data/estabelecimento.parquet",