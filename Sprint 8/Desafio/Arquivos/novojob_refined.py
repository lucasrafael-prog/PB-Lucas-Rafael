# Importação das bibliotecas
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import regexp_replace, col, when, lit, posexplode_outer, explode_outer, split, to_date, year, month, dayofmonth, floor, trim
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

# Limpeza de dados do TMDB
df_tmdb_trusted = spark.read.parquet(source_json_path)

# Criação das colunas de atributos de data para a tabela
df = df_tmdb_trusted \
    .withColumn("release_date", to_date(col("release_date"), "yyyy-MM-dd")) \
    .withColumn("ano_lancamento", year(col("release_date"))) \
    .withColumn("mes_lancamento", month(col("release_date"))) \
    .withColumn("dia_lancamento", dayofmonth(col("release_date"))) \
    .withColumn("semestre", when(month(col("release_date")) <= 6, 1).otherwise(2)) \
    .withColumn("decada", floor(col("ano_lancamento") / 10) * 10)

# Explodindo ID do gênero
df_generos_ids = df.select("*", posexplode_outer("id_genres").alias("pos", "id_genero"))

# Explodindo Conteúdo de Gêneros
df_generos_tmdb = df_generos_ids.select("*", posexplode_outer("genres").alias("pos2", "genero"))


# Juntando os gêneros e Explodindo as Palavras-Chave (Excluindo colunas criadas para posicionamento dos dados de gênero)
df_tmdb_explodido = df_generos_tmdb \
    .filter(col("pos") == col("pos2")) \
    .drop("pos", "pos2") \
    .withColumn("palavra_chave", explode_outer(col("keywords")))
    
# Criação da Tabela Fato Filmes com suas colunas
fato_avaliacao_filmes_tmdb = df_tmdb_explodido.select(
    col("movie_id").alias("id_filme"),
    col("vote_average").alias("nota_media"),
    col("vote_count").alias("qtd_votos"),
    col("popularity").alias("popularidade"),
    col("release_date").alias("data_lancamento"),
    col("id_genero"),
    col("id_collection").alias("id_franquia")
)


# Criação da tabela  Dimensão Filmes com suas colunas
dim_filmes_tmdb = df_tmdb_explodido.select(
    col("movie_id").alias("id_filme"),
    col("title").alias("titulo"),
    col("overview").alias("descricao"),
    col("palavra_chave")
)

# Criação da Tabela Dimensão Gênero com suas colunas
dim_genero_tmdb = df_tmdb_explodido.select(
    col("id_genero"),
    col("genero")
).dropDuplicates()



# Criação da Tabela Dimensão Franquia com suas colunas
dim_franquia_tmdb = df_tmdb_explodido.select(
    col("id_collection").alias("id_franquia"),
    col("collection").alias("franquia")
).dropna().dropDuplicates()



# Criação da Tabela Dimensão Data com suas colunas
dim_data_tmdb = df_tmdb_explodido.select(
    col("release_date").alias("data_lancamento"),
    col("ano_lancamento"),
    col("mes_lancamento"),
    col("dia_lancamento"),
    col("semestre"),
    col("decada")
).dropDuplicates()



# Limpeza de dados do CSV
df_local_trusted = spark.read.parquet(source_csv_path)

# Renomeando ID 
df_local = df_local_trusted.withColumnRenamed('id', 'id_filme')

# Removendo índice de letra(tt) do ID
df_local = df_local.withColumn('id_filme', regexp_replace('id_filme', 'tt', ''))

# Substituindo valores nulos por None
df_local = df_local.select([when(col(c) == "\\N", lit(None)).otherwise(col(c)).alias(c) for c in df_local.columns])

# Convertendo tipos de dados
df_local = df_local.withColumn('id_filme', col('id_filme').cast(IntegerType())) \
    .withColumn('anoLancamento', col('anoLancamento').cast(IntegerType())) \
    .withColumn('tempoMinutos', col('tempoMinutos').cast(IntegerType())) \
    .withColumn('notaMedia', col('notaMedia').cast(FloatType())) \
    .withColumn('numeroVotos', col('numeroVotos').cast(IntegerType())) \
    .withColumn('anoNascimento', col('anoNascimento').cast(IntegerType())) \
    .withColumn('anoFalecimento', col('anoFalecimento').cast(IntegerType()))
    
# Explosão dos arrays de gênero
df_local = df_local \
    .withColumn("genero_explodido", explode_outer(split(col("genero"), ","))) \
    .withColumn("genero_explodido", trim(col("genero_explodido"))) \
    .filter(col("genero_explodido").isin(["Crime", "War"])) \
    .withColumn("id_genero", when(col("genero_explodido") == "Crime", 80)
                            .when(col("genero_explodido") == "War", 10752))
    
    
    
# Criação da tabela Fato_Avaliacao_Filme Local(Arquivo CSV)   
fato_avaliacao_filmes_local = df_local.select(
    col("id_filme"),
    col("notaMedia").alias("nota_media"),
    col("numeroVotos").alias("qtd_votos"),
    lit(None).cast("double").alias("popularidade"),
    to_date(col("anoLancamento").cast("string"), "yyyy").alias("data_lancamento"),
    col("id_genero"),
    lit(None).cast("int").alias("id_franquia")
    
)


# Criacão da tabela Dimensão Filmes Local
dim_filmes_local = df_local.select(
    col("id_filme"),
    col("tituloPincipal").alias("titulo"),
    lit(None).cast("string").alias("descricao"),
    lit(None).cast("string").alias("palavra_chave")
).dropna().dropDuplicates()


# Criação da tabela Dimensão Gênero Local 
dim_genero_local = df_local.select(
    col("id_genero"),
    col("genero_explodido").alias("genero")
).dropna().dropDuplicates()



# Criação da tabela Dimensão Data Local
dim_data_local = df_local.select(
    to_date(col("anoLancamento").cast("string"), "yyyy").alias("data_lancamento"),
    col("anoLancamento").alias("ano_lancamento") ,
    lit(None).cast("int").alias("mes_lancamento"),
    lit(None).cast("int").alias("dia_lancamento"),
    lit(None).cast("int").alias("semestre"),
).dropna().dropDuplicates() \
    .withColumn("decada", floor(col("ano_lancamento") / 10) * 10)
    


# Unificando as tabelas Tmdb e Locais
fato_avaliacao_filmes = fato_avaliacao_filmes_tmdb.unionByName(fato_avaliacao_filmes_local).dropDuplicates()
dim_filmes = dim_filmes_tmdb.unionByName(dim_filmes_local)
dim_genero = dim_genero_tmdb.unionByName(dim_genero_local).dropDuplicates()
dim_franquia = dim_franquia_tmdb
dim_data = dim_data_tmdb.unionByName(dim_data_local).dropDuplicates()

# Apresentacao das tabelas para verificação dos logs
fato_avaliacao_filmes.show()
dim_filmes.show()
dim_genero.show()
dim_franquia.show(10)
dim_data.show(10)



# Salvando as tabelas em Parquet nos caminhos
# Tabelas
fato_avaliacao_filmes.write.mode("overwrite").parquet(target_path + "Fato_Avaliacao_Filmes")
dim_filmes.write.mode("overwrite").parquet(target_path + "Dim_Filmes")
dim_genero.write.mode("overwrite").parquet(target_path + "Dim_Genero")
dim_franquia.write.mode("overwrite").parquet(target_path + "Dim_Franquia")
dim_data.write.mode("overwrite").parquet(target_path + "Dim_Data")

job.commit()