import os

import polars as pl

RAW_FOLDER = '../../data/pncp_contratos/raw'
BRONZE_FOLDER = '../../data/pncp_contratos/bronze'

if __name__ == '__main__':
    df = pl.read_parquet(RAW_FOLDER)
    if not os.path.isdir(BRONZE_FOLDER):
        os.makedirs(BRONZE_FOLDER)
    print(df)
    df.write_parquet(os.path.join(BRONZE_FOLDER, 'pncp_contratos.parquet'))
