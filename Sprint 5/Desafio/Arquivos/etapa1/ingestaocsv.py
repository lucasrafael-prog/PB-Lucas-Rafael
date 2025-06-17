# Immportação das Bibliotecas
import pandas as pd
import boto3
import os
from dotenv import load_dotenv
from datetime import datetime


# Leitura dos Arquivos
df_mv = pd.read_csv('data/movies.csv', encoding='utf-8', sep='|', low_memory=False)
print(f"Arquivo Movies lido com sucesso!")
df_sr = pd.read_csv('data/series.csv', encoding='utf-8', sep='|', low_memory=False)
print(f"Arquivo Series lido com sucesso!")


# Utilização do dotenv para carregar as chaves de acesso
load_dotenv()

# Conexão do S3 com as Access Keys
s3 = boto3.client('s3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    aws_session_token=os.getenv('AWS_SESSION_TOKEN'),
    region_name='us-east-1'
)
print(f'Conexão com AWS iniciada.')

# Nome do Bucket
bucket_name = 'datalake-lucasrafael'

# Data para o caminho
hoje = datetime.now()
caminho_data = f"{hoje.year}/{hoje.month:02}/{hoje.day:02}"

# Nome dos Arquivos locais
arquivo_movies_local = 'data/movies.csv'
arquivo_series_local = 'data/series.csv'

# Nome dos Arquivos no Bucket
file1_s3_name = 'movies.csv'
file2_s3_name = 'series.csv'

# Caminhos dos Arquivos para upload
caminho_filmes = f"Raw/Local/CSV/Movies/{caminho_data}/{file1_s3_name}"
caminho_series = f"Raw/Local/CSV/Series/{caminho_data}/{file2_s3_name}"


# Upload Movies
s3.upload_file(arquivo_movies_local, bucket_name, caminho_filmes)
print(f'Movies enviado para: s3://{bucket_name}/{caminho_filmes}')

# Upload Series
s3.upload_file(arquivo_series_local, bucket_name, caminho_series)
print(f'Series enviado para: s3://{bucket_name}/{caminho_series}')