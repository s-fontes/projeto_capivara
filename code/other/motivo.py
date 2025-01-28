import pandas as pd

df_motivo = pd.read_csv('motivo.csv', sep=";", encoding='latin1')
df_motivo.to_parquet('motivo.parquet', index=False, engine='pyarrow')
