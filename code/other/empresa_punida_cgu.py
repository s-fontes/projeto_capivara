import pandas as pd

df_ep = pd.read_csv('empresas_punidas_cgu.csv', sep=";", encoding='latin1')
df_ep.to_parquet('empresas_punidas_cgu.parquet', index=False, engine='pyarrow')
