import json
import os

import polars as pl

RAW_FOLDER = '../../data/fornecedores_fisica/raw'
BRONZE_FOLDER = '../../data/fornecedores_fisica/bronze'

if __name__ == '__main__':
    files = os.listdir(RAW_FOLDER)
    dfs = []
    for file in files:
        with open(os.path.join(RAW_FOLDER, file)) as f:
            data = json.load(f)
            df = pl.json_normalize(data)
            dfs.append(df)
    df = pl.concat(dfs)
    if not os.path.isdir(BRONZE_FOLDER):
        os.makedirs(BRONZE_FOLDER)
    print(df)
    df.write_parquet(os.path.join(BRONZE_FOLDER, 'fornecedores_fisica.parquet'))
