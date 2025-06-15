create
or replace table dataset.contratos_compras as
select
    cc.*,
    bf.bolsa_familia,
    bf.valor_total as valor_total_bolsa_familia,
    c.candidato,
    c.total_despesa as total_despesa_candidato,
    c.total_receita as total_receita_candidato,
    ccand.contratado_candidato,
    ccand.total_contratado as valor_total_contratado_candidato,
    cp.contratado_partido,
    cp.total_contratado as valor_total_contratado_partido,
    dc.doador_candidato,
    dc.total_doado as valor_total_doado_candidato,
    doc.doador_originario_candidato,
    doc.total_doado as valor_total_doado_originario_candidato,
    dp.doador_partido,
    dp.total_doado as valor_total_doado_partido,
    ic.inidonea_cgu,
    ic.total_sancoes as total_sancoes_inidonea_cgu,
    it.inidonea_tcu,
    it.total_sancoes as total_sancoes_inidonea_tcu,
    pc.punida_cgu,
    pc.total_sancoes as total_sancoes_punida_cgu,
    sbf.socio_bolsa_familia,
    coalesce(sbf.valor_total, 0) as valor_total_socio_bolsa_familia,
    sc.socio_candidato,
    coalesce(sc.total_despesa, 0) as total_despesa_socio_candidato,
    coalesce(sc.total_receita, 0) as total_receita_socio_candidato,
    scc.socio_contratado_candidato,
    coalesce(scc.total_contratado, 0) as valor_total_socio_contratado_candidato,
    scp.socio_contratado_partido,
    coalesce(scp.total_contratado, 0) as valor_total_socio_contratado_partido,
    sdc.socio_doador_candidato,
    coalesce(sdc.total_doado, 0) as valor_total_socio_doador_candidato,
    sdoc.socio_doador_originario_candidato,
    coalesce(sdoc.total_doado, 0) as valor_total_socio_doador_originario_candidato,
    sdp.socio_doador_partido,
    coalesce(sdp.total_doado, 0) as valor_total_socio_doador_partido,
    sic.socio_inidonea_cgu,
    coalesce(sic.total_sancoes, 0) as total_sancoes_socio_inidonea_cgu,
    spc.socio_punida_cgu,
    coalesce(spc.total_sancoes, 0) as total_sancoes_socio_punida_cgu,
    ei.*
from
    main.contratos_compras as cc
    left join  flags.bolsa_familia as bf on cc.id = bf.id
    left join flags.candidato as c on cc.id = c.id
    left join flags.contratado_candidato as ccand on cc.id = ccand.id
    left join flags.contratado_partido as cp on cc.id = cp.id
    left join flags.doador_candidato as dc on cc.id = dc.id
    left join flags.doador_originario_candidato as doc on cc.id = doc.id
    left join flags.doador_partido as dp on cc.id = dp.id
    left join flags.inidonea_cgu as ic on cc.id = ic.id
    left join flags.inidonea_tcu as it on cc.id = it.id
    left join flags.punida_cgu as pc on cc.id = pc.id
    left join flags.socio_bolsa_familia as sbf on cc.id = sbf.id
    left join flags.socio_candidato as sc on cc.id = sc.id
    left join flags.socio_contratado_candidato as scc on cc.id = scc.id
    left join flags.socio_contratado_partido as scp on cc.id = scp.id
    left join flags.socio_doador_candidato as sdc on cc.id = sdc.id
    left join flags.socio_doador_originario_candidato as sdoc on cc.id = sdoc.id
    left join flags.socio_doador_partido as sdp on cc.id = sdp.id
    left join flags.socio_inidonea_cgu as sic on cc.id = sic.id
    left join flags.socio_punida_cgu as spc on cc.id = spc.id
    left join flags.empresa_info as ei on regexp_replace(fornecedor_cnpj_cpf_idgener, '\D', '', 'g') = ei.cnpj