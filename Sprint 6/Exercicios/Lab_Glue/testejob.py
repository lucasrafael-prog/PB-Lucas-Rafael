import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark import SparkContext, SQLContext
from pyspark.sql.functions import upper, col, desc, asc, count, sum

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_TARGET_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

source_file = args['S3_INPUT_PATH']
target_path = args['S3_TARGET_PATH']

# Leitura do Arquivo
my_dynamic_frame = glueContext.create_dynamic_frame.from_options(
    "s3",
    {
        "paths": [
            source_file
            ]
    },
    "csv",
    {"withHeader": True, "separator":","}
    )
    


# Consultas
# Impressão do Schema do DF
my_dynamic_frame.printSchema()

spark_df = my_dynamic_frame.toDF()

# Alterando a caixa para Maiúsculo
spark_df = spark_df.withColumn("nome", upper(col("nome")))

# Contagem de linhas do DF
spark_df.agg(count("*").alias("total_linhas")).show()

# Contagem de registros agrupando por ano e sexo
spark_df.groupBy("ano", "sexo").agg(count(col("nome")).alias("total_pessoas")).orderBy(desc(col("ano"))).show()

# Nome feminino com mais registros e ano que ocorreu
df_feminino = spark_df.filter(col("sexo") == "F").groupBy("nome", "ano").agg(sum("total").alias("total_registros")) \
    .orderBy(desc(col("total_registros"))).show(1)

# Nome masculino com mais registros e ano que ocorreu
df_masculino = spark_df.filter(col("sexo") == "M").groupBy("nome", "ano").agg(sum("total").alias("total_registros")) \
    .orderBy(desc(col("total_registros"))).show(1)

# Registros para cada ano presente no DataFrame
spark_df.groupBy("ano").agg(sum("total").alias('total_ano')).orderBy(asc(col("ano"))).limit(10).show()

# Transformação do DataFrame em DinamycFrame
df_final = DynamicFrame.fromDF(spark_df, glueContext, "df_final")
    
# Escrevendo o conteúdo do DF no S3
glueContext.write_dynamic_frame.from_options(
        frame = df_final,
        connection_type = "s3",
        connection_options = {
            "path": target_path,
            "partitionKeys": ["sexo", "ano"]
            },
        format = "json"
        )

job.commit()