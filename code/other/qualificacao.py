import pandas as pd

df_qualificacao = pd.read_csv('qualificacao.csv', sep=";", encoding='latin1')
df_qualificacao.to_parquet('qualificacao.parquet', index=False, engine='pyarrow')
