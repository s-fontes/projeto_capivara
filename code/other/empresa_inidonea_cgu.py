import pandas as pd

df_eic = pd.read_csv('empresas_inidoneas_cgu.csv', sep=";", encoding='latin1')
df_eic.to_parquet('empresa_inidonea_cgu.parquet', index=False, engine='pyarrow')
