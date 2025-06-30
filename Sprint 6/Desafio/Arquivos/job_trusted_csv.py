import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext() # Criação do contexto Spark
glueContext = GlueContext(sc) # Criação do contexto Glue
spark = glueContext.spark_session # Criação da sessão Spark
job = Job(glueContext) # Criação do objeto Job
job.init(args['JOB_NAME'], args) # Inicialização do Job


# Leitura do arquivo CSV no S3
df = glueContext.create_dynamic_frame.from_options( # Criação do DynamicFrame
    connection_type="s3", # Tipo de conexão S3
    connection_options={ # Opções de conexão
        "paths": [ # Caminho do arquivo CSV no S3
            "s3://datalake-lucasrafael/Raw/Local/CSV/Movies/"
            ],
        "recurse": True # Leitura recursiva de subdiretórios
    },
    format="csv", # Formato do arquivo
    format_options={"withHeader": True, "separator":"|"} # Opções de formatação 
    )

# Enviando para a camada Trusted
glueContext.write_dynamic_frame.from_options( # Escrita do DynamicFrame no S3
        frame = df, # Frame a ser escrito
        connection_type = "s3", # Tipo de conexão S3
        connection_options = { # Opções de conexão
            "path": "s3://datalake-lucasrafael/Trusted/Local/Parquet/Movies/", # Caminho de destino no S3
            "partitionKeys": [] # Nenhuma chave de particionamento
            },
        format = "parquet" # Formato de sáida do arquivo
        )

job.commit() # Commit do Job finalizando a execução