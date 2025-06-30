import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import input_file_name, regexp_extract, explode, from_json
from pyspark.sql.types import *

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','S3_INPUT_PATH', 'S3_TARGET_PATH'])

sc = SparkContext() # Criação do contexto Spark
glueContext = GlueContext(sc) # Criação do contexto Glue
spark = glueContext.spark_session # Criação da sessão Spark
job = Job(glueContext) # Criação do objeto Job
job.init(args['JOB_NAME'], args) # Inicialização do Job

source_file = args['S3_INPUT_PATH'] # Caminho dos arquivos do S3
target_path = args['S3_TARGET_PATH'] # Caminho de destino dos arquivos do S3

# Criando Schema manualmente definido para os arquivos JSON
movie_schema = StructType([
        StructField("movie_id", LongType(), True),
        StructField("title", StringType(), True),
        StructField("overview", StringType(), True),
        StructField("release_date", StringType(), True),
        StructField("popularity", DoubleType(), True),
        StructField("vote_average", DoubleType(), True),
        StructField("vote_count", LongType(), True),
        StructField("collection", StringType(), True),
        StructField("genres", ArrayType(StringType()), True),
        StructField("keywords", ArrayType(StringType()), True)
    ])



# Lendo arquivos JSON diretamente da Camada Raw com Spark
df_raw = spark.read.text(source_file) # Lendo em texto

# Transformando o DataFrame de texto em JSON
df_json = df_raw.select(
    from_json(df_raw.value, ArrayType(movie_schema)).alias("filmes"),
    input_file_name().alias("file_path")
)

# Explodindo o array de filmes para que cada filme seja uma linha
df = df_json.select(explode("filmes").alias("row"), "file_path").select("row.*", "file_path")

df.show(truncate=False) # Exibindo o DataFrame para conferência

df = df.withColumn("file_path", input_file_name()) # Adicionando coluna com o caminho do arquivo

# Extração do ano, mês e dia do caminho do arquivo
df = df.withColumn("ano", regexp_extract("file_path", r"/(\d{4})/", 1)) # Uso do regex para extração do ano
df = df.withColumn("mes", regexp_extract("file_path", r"/\d{4}/(\d{2})/", 1)) # Uso do regex para extração do mês
df = df.withColumn("dia", regexp_extract("file_path", r"/\d{4}/\d{2}/(\d{2})/", 1)) # Uso do regex para extração do dia

df.select("file_path", "ano", "mes", "dia").distinct().show(truncate=False) # Acessando os dados para conferir se foram passados

# Transformando o DataFrame de volta para DynamicFrame
df_final = DynamicFrame.fromDF(df, glueContext, "df_final") 

# Enviando para a camada Trusted
glueContext.write_dynamic_frame.from_options( # Escrita do DynamicFrame no S3
        frame = df_final, # Frame a ser escrito
        connection_type = "s3", # Tipo de conexão S3
        connection_options = { # Opções de conexão
            "path": target_path, # Caminho de destino no S3
            "partitionKeys": ["ano", "mes", "dia"] # Chaves de particionamento
            },
        format = "parquet" # Formato de saída do arquivo
        )

job.commit() # Commit do Job finalizando a execução