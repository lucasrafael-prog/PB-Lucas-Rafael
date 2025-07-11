# Importação das bibliotecas
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import regexp_replace, col, when, lit, posexplode_outer, explode_outer, split, to_date, year, month, dayofmonth, when, floor
from pyspark.sql.types import IntegerType, FloatType

# Inicialização do Job
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_LOCAL_PATH', 'S3_API_PATH', 'S3_TARGET_PATH'])

# Criação dos contextos Glue e Spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Caminhos de origem e destino dos arquivos
source_csv_path = args['S3_LOCAL_PATH']
source_json_path = args['S3_API_PATH']
target_path = args['S3_TARGET_PATH']


# Limpeza de dados do CSV
df_local_trusted = spark.read.parquet(source_csv_path)

# Renomeando ID 
df_local = df_local_trusted.withColumnRenamed('id', 'filme_id')

# Removendo índice de letra(tt) do ID
df_local = df_local.withColumn('filme_id', regexp_replace('filme_id', 'tt', ''))

# Substituindo valores nulos por None
df_local = df_local.select([when(col(c) == "\\N", lit(None)).otherwise(col(c)).alias(c) for c in df_local.columns])

# Convertendo tipos de dados
df_local = df_local.withColumn('filme_id', col('filme_id').cast(IntegerType())) \
    .withColumn('anoLancamento', col('anoLancamento').cast(IntegerType())) \
    .withColumn('tempoMinutos', col('tempoMinutos').cast(IntegerType())) \
    .withColumn('notaMedia', col('notaMedia').cast(FloatType())) \
    .withColumn('numeroVotos', col('numeroVotos').cast(IntegerType())) \
    .withColumn('anoNascimento', col('anoNascimento').cast(IntegerType())) \
    .withColumn('anoFalecimento', col('anoFalecimento').cast(IntegerType()))

# Criação da tabela Fato_Filme Local(Arquivo CSV)   
fato_filmes_local = df_local.select(
    col("filme_id"),
    col("tituloPincipal").alias("titulo"),
    col("notaMedia").alias("nota_media"),
    col("numeroVotos").alias("qtd_votos")
)

fato_filmes_local.show(10)

# Criação da tabela Dimensão Gênero Local 
dim_genero_local = df_local.select(
    col("genero")
).dropna().dropDuplicates()

dim_genero_local.show()

# Criação da tabela Dimensão Data Local
dim_data_local = df_local.select(
    col("anoLancamento").alias("ano_lancamento")
).dropna().dropDuplicates() \
    .withColumn("decada", floor(col("ano_lancamento") / 10) * 10)
    
dim_data_local.show(10)

# Limpeza de dados do TMDB
df_trusted = spark.read.parquet(source_json_path)

# Criação das colunas de atributos de data para a tabela
df = df_trusted \
    .withColumn("release_date", to_date(col("release_date"), "yyyy-MM-dd")) \
    .withColumn("ano_lancamento", year(col("release_date"))) \
    .withColumn("mes_lancamento", month(col("release_date"))) \
    .withColumn("dia_lancamento", dayofmonth(col("release_date"))) \
    .withColumn("semestre", when(month(col("release_date")) <= 6, 1).otherwise(2)) \
    .withColumn("decada", floor(col("ano_lancamento") / 10) * 10)

# Explodindo ID do gênero
df_generos_ids = df.select("*", posexplode_outer("id_genres").alias("pos", "id_genero"))

# Explodindo Conteúdo de Gêneros
df_generos = df_generos_ids.select("*", posexplode_outer("genres").alias("pos2", "genero"))

# Juntando os gêneros e Explodindo as Palavras-Chave (Excluindo colunas criadas para posicionamento dos dados de gênero)
df_explodido = df_generos \
    .filter(col("pos") == col("pos2")) \
    .drop("pos", "pos2") \
    .withColumn("palavra_chave", explode_outer(col("keywords")))
    
# Criação da Tabela Fato Filmes com suas colunas
fato_filmes = df_explodido.select(
    col("movie_id").alias("filme_id"),
    col("title").alias("titulo"),
    col("overview").alias("descricao"),
    col("vote_average").alias("nota_media"),
    col("vote_count").alias("qtd_votos"),
    col("popularity").alias("popularidade"),
    col("palavra_chave"),
    col("release_date").alias("data_lancamento"),
    col("id_genero"),
    col("id_collection").alias("id_franquia")
)

fato_filmes.show(10)

# Criação da Tabela Dimensão Gênero com suas colunas
dim_genero = df_explodido.select(
    col("id_genero"),
    col("genero")
).dropDuplicates()

dim_genero.show(10)

# Criação da Tabela Dimensão Franquia com suas colunas
dim_franquia = df_explodido.select(
    col("id_collection").alias("id_franquia"),
    col("collection").alias("franquia")
).dropna().dropDuplicates()

dim_franquia.show(10)

# Criação da Tabela Dimensão Data com suas colunas
dim_data = df_explodido.select(
    col("release_date").alias("data_lancamento"),
    col("ano_lancamento"),
    col("mes_lancamento"),
    col("dia_lancamento"),
    col("semestre"),
    col("decada")
).dropDuplicates()

dim_data.show(10)

# Salvando as tabelas em Parquet nos caminhos
# Tabelas TMDB
fato_filmes.write.mode("overwrite").parquet(target_path + "Fato_Filmes")
dim_genero.write.mode("overwrite").parquet(target_path + "Dim_Genero")
dim_franquia.write.mode("overwrite").parquet(target_path + "Dim_Franquia")
dim_data.write.mode("overwrite").parquet(target_path + "Dim_Data")

# Tabelas Locais
fato_filmes_local.write.mode("overwrite").parquet(target_path + "Fato_Filmes_Local")
dim_genero_local.write.mode("overwrite").parquet(target_path + "Dim_Genero_Local")
dim_data_local.write.mode("overwrite").parquet(target_path + "Dim_Data_Local")

# Criação das Views para teste
# Views TMDB
fato_filmes.createOrReplaceTempView("fato_filmes_v")
dim_genero.createOrReplaceTempView("dim_genero_v")
dim_franquia.createOrReplaceTempView("dim_franquia_v")
dim_data.createOrReplaceTempView("dim_data_v")

# Views Local 
fato_filmes_local.createOrReplaceTempView("fato_filmes_local_v")
dim_genero_local.createOrReplaceTempView("dim_genero_local_v")
dim_data_local.createOrReplaceTempView("dim_data_local_v")

job.commit()