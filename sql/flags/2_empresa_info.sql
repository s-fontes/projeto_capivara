create table flags.empresa_info as
select
    *
from
    main.empresa as emp
    left join main.estabelecimento as est on emp.cnpj = est.cnpj