import pandas as pd

df_ei = pd.read_csv('empresas_inidoneas.csv', sep=";", encoding='latin1')
df_ei.columns = [
    "nome_responsavel", "cpf_cnpj", "uf", "processo", "deliberacao", "transito_julgado", "data_final", "data_acordao"]


def format_cnpj(cnpj):
    return cnpj.replace(".", "").replace("/", "").replace("-", "")


df_ei["cpf_cnpj"] = df_ei["cpf_cnpj"].apply(format_cnpj)
df_ei.to_parquet('empresas_inidoneas.parquet', index=False, engine='pyarrow')
