import pandas as pd

df_cnae = pd.read_csv('cnae.csv', sep=";", encoding='latin1')
df_cnae.to_parquet('cnae.parquet', index=False, engine='pyarrow')
