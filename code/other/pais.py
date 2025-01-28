import pandas as pd

df_pais = pd.read_csv('pais.csv', sep=";", encoding='latin1')
df_pais.to_parquet('pais.parquet', index=False, engine='pyarrow')
