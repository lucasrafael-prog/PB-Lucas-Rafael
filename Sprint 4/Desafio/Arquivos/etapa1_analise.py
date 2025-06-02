import pandas as pd # Importação Pandas
from dotenv import load_dotenv # Importação .env
import boto3 # Importação boto3
import os # Importação OS

df = pd.read_csv('Consultas_Ambulatoriais_(Referencia_JanFevMar_de_2025).csv', encoding='ISO-8859-1') # Leitura do CSV de padrão antigo


# Utilização do dotenv para carregar as chaves de acesso
load_dotenv()

# Conexão do S3 com as Access Keys
s3 = boto3.client('s3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    aws_session_token=os.getenv('AWS_SESSION_TOKEN'),
    region_name='us-east-1'
)

bucket_name = 'sprint4bucket' # Nome do Bucket 
file_name = 'Consultas_Ambulatoriais_(Referencia_JanFevMar_de_2025).csv' # Arquivo Original 
object_name = 'consultas_ambulatoriais_original.csv' # Nome do Arquivo no Bucket

s3.upload_file(file_name, bucket_name, object_name) # Upload do Arquivo