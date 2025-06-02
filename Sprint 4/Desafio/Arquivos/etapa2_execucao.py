import pandas as pd # Importação do Pandas
import numpy as np # Importação do Numpy
import boto3 # Importação do boto3
import os # Interação com o SO
from dotenv import load_dotenv # Importação do .env
from io import StringIO # Importação do StringIO

# Leitura dos Dados
load_dotenv() # Reconhece o arquivo .env

# Conexão do S3 com as Access Keys
s3 = boto3.client('s3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    aws_session_token=os.getenv('AWS_SESSION_TOKEN'),
    region_name='us-east-1'
)

bucket_name = 'sprint4bucket' # Identifica o bucket
object_name = 'consultas_ambulatoriais_original.csv' # Identifica o arquivo

response = s3.get_object(Bucket=bucket_name, Key=object_name) # Busca os objetos na AWS
content = response['Body'].read().decode('ISO-8859-1') # Mostra como deve ler o conteúdo

df = pd.read_csv(StringIO(content), sep=';') # StringIO possibilita a leitura do arquivo, separando as colunas por ';'

# Manipulação dos dados

# Questionamento 1:

df['idade'] = df['idade'].fillna(0).astype(int) # Convertendo coluna idade em int

# Função condicional para criar a coluna faixa etária
df['faixa etária'] = np.where(df['idade'] <= 12, 'Criança',
np.where(df['idade'] <= 17, 'Adolescente',
np.where(df['idade'] <= 59, 'Adulto', 'Idoso')))

# Separando a data e a hora em colunas com a função de string
df[['data_consulta', 'hora_consulta']] = df['data_consulta'].str.split(' ', expand=True) # A separação ocorre no espaço 

# Aplicando a função de conversão para data
df['data_consulta'] = pd.to_datetime(df['data_consulta'], format='%d/%m/%Y')

# Utilizando função de string para capitalizar os textos da coluna especialidade
df['especialidade'] = df['especialidade'].str.title()

# Realizando a filtragem dos itens que possuem cirurgia em janeiro (filtro & e ==)
cirurgia_idade = df[df['especialidade'].str.contains('Cirurgia') & df['faixa etária'] & df['data_consulta'].dt.month == 1]

# Demonstrando o resultado da filtragem com a função de agregação para contagem
resultado = cirurgia_idade.value_counts('faixa etária').sort_values(ascending=False)
print(resultado)

# Questionamento 2:

# Função de string para que apareça o sexo
df['sexo'] = df['sexo'].str.replace('M', 'Masculino')
df['sexo'] = df['sexo'].str.replace('F', 'Feminino')
df['sexo'] = df['sexo'].fillna('Não informado')

# Agrupando por especialidade e sexo
distribuicao_atend = df.groupby(['especialidade', 'sexo']).size().reset_index(name='quantidade')

# Função de String para converter os dados com 'I' em não informado
df['sexo'] = df['sexo'].str.replace('I', 'Não informado')

# Função de agregação para mostrar a distribuição masculina
distrib_masc = df[df['sexo'] == 'Masculino'].value_counts(['especialidade', 'sexo']).reset_index(name='quantidade').head(10)
print(distrib_masc)

# Função de agregação para mostrar a distribuição feminina
distrib_femi = df[df['sexo'] == 'Feminino'].value_counts(['especialidade', 'sexo']).reset_index(name='quantidade').head(10)
print(distrib_femi)

# Questionamento 3:

# Utilizando função de string para capitalizar os textos da coluna município
df['município'] = df['município'].str.title()

# Função para completar os valores da tabela
df['município'] = df['município'].fillna('Não informado')

# Função de agregação para filtragem dos dados
pacientes_psiq = df[df['especialidade'] == 'Psiquiatria'].value_counts('município').reset_index(name='quantidade')
print(pacientes_psiq.sort_values(by='quantidade', ascending=False).head(10))

# Exportação dos dados
df.to_csv('Consultas_Ambulatoriais_tratado.csv', index=False, encoding='utf-8') # Transformando o arquivo em CSV

# Conexão com o Bucket
file_name = 'Consultas_Ambulatoriais_tratado.csv' # Nome do arquivo tratado
object_name = 'consultas_ambulatoriais_tratado.csv' # Nome do arquivo no bucket
s3.upload_file(file_name, bucket_name, object_name) # Upload do Arquivo

response = s3.get_object(Bucket=bucket_name, Key=object_name) # Busca o CSv tratado na AWS
content = response['Body'].read().decode('utf-8') # Mostra como deve ler o conteúdo
df = pd.read_csv(StringIO(content)) # Leitura do arquivo

print(df)