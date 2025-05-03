import pandas as pd

df_municipio = pd.read_csv('municipio.csv', sep=";", encoding='latin1')
df_municipio.to_parquet('municipio.parquet', index=False, engine='pyarrow')
