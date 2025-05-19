#Importação dos Dados
import pandas as pd

# Etapa 1 - ETL

# Extração dos Dados
dados = pd.read_csv("concert_tours_by_women.csv")

# Transformação dos Dados

# Excluindo Colunas desnecessárias
dados.drop(columns=['Peak', 'All Time Peak', 'Ref.'], inplace=True)

# Removendo símbolos das colunas numéricas (utilização dos atributos de Regex)
dados['Actual gross'] = dados['Actual gross'].str.replace("[$,]|[]|[a-z]", "", regex=True) # Coluna Actual gross
dados['Adjustedgross (in 2022 dollars)'] = dados['Adjustedgross (in 2022 dollars)'].str.replace("[$,]", "", regex=True) # Coluna Adjusted gross
dados['Average gross'] = dados['Average gross'].str.replace("[$,]", "", regex=True) # Coluna Average gross

# Convertendo as colunas de string para float
dados['Actual gross'] = dados['Actual gross'].astype(float) # Coluna Actual gross
dados['Adjustedgross (in 2022 dollars)'] = dados['Adjustedgross (in 2022 dollars)'].astype(float) # Coluna Adjusted gross
dados['Average gross'] = dados['Average gross'].astype(float) # Coluna Average gross

# Renomeando Coluna Adjusted gross - Inserindo espaço que falta.
dados.rename(columns={"Adjustedgross (in 2022 dollars)": "Adjusted gross (in 2022 dollars)"}, inplace=True) # Comando para renomear

# Removendo símbolos e itens nos colchetes da coluna Tour title
dados['Tour title'] = dados['Tour title'].str.replace(r"\[[a-z0-9]+\]|[*‡†.:]","", regex=True) # Removendo com uso do regex

# Criando novas colunas para separar os anos 
dados[['Start year', 'End year']] = dados['Year(s)'].str.split('-', expand=True) # Separando as colunas pelo '-' e expand
dados['End year'] = dados['End year'].fillna(dados['Start year']) # Completando a coluna End year, inserindo os dados da Start year quando só ha o ano de início

# Convertendo as novas colunas em formato Int excluindo a coluna Year(s)
dados[['Start year','End year']] = dados[['Start year','End year']].astype(int) # Conversão
dados.drop(columns='Year(s)', inplace=True) # Exclusão

# Convertendo o valor da coluna Shows para int
dados['Shows'] = dados['Shows'].astype(int) 

# Finalizada a Limpeza

# Carregamento dos Dados
dados.to_csv('volume/csv_limpo.csv', index=False)

