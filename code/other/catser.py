import pandas as pd

df_catser = pd.read_csv('catser.csv', sep=";", encoding='latin1')
df_catser.to_parquet('catser.parquet', index=False, engine='pyarrow')
