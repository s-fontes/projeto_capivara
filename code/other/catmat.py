import pandas as pd

df_catmat = pd.read_csv('catmat.csv', sep=";", encoding='latin1')
df_catmat.to_parquet('catmat.parquet', index=False, engine='pyarrow')
