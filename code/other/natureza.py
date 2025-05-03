import pandas as pd

df_natureza = pd.read_csv('natureza.csv', sep=";", encoding='latin1')
df_natureza.to_parquet('natureza.parquet', index=False, engine='pyarrow')
