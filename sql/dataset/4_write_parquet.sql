COPY (
    SELECT
        *
    FROM
        dataset.contratos_compras
) TO './dataset.parquet' (FORMAT parquet);