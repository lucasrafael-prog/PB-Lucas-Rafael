# Importações
from pyspark.sql import SparkSession
from pyspark import SparkContext, SQLContext
from pyspark.sql.functions import array, lit, rand, floor, element_at, count

# Etapa 1 - Criação da sessão Spark
spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Manipulacao DataFrame") \
    .getOrCreate()
    
df_nomes = spark.read.csv("nomes_aleatorios.txt", header=False, sep="\n", inferSchema=True)

df_nomes.show()

# Etapa 2 - Renomeando a coluna
df_nomes = df_nomes.withColumnRenamed("_c0", "Nomes")

df_nomes.printSchema()
df_nomes.show(10)

# Etapa 3 - Adicionando coluna Escolaridade
valores_escolaridade = array(
    lit("Fundamental"),
    lit("Médio"),
    lit("Superior")
)
indices_escola = (floor(rand() * 3) + 1).cast('int') 
df_nomes = df_nomes.withColumn("Escolaridade", element_at(valores_escolaridade, indices_escola))

df_nomes.show()

# Etapa 4 - Adicionando coluna País
valores_paises = array(
    lit("Brasil"),
    lit("Argentina"),
    lit("Colômbia"),
    lit("Chile"),
    lit("Uruguai"),
    lit("Paraguai"),
    lit("Bolívia"),
    lit("Peru"),
    lit("Venezuela"),
    lit("Equador"),
    lit("Suriname"),
    lit("Guiana"),
    lit("Guiana Francesa")
)

indices_paises = (floor(rand() * 13) + 1).cast('int')
df_nomes = df_nomes.withColumn("Pais", element_at(valores_paises, indices_paises))

df_nomes.show()

# Etapa 5 - Adicionando coluna AnoNascimento
indices_anos = (floor(rand() * 66) + 1945).cast('int')  # Anos de 1945 a 2010
df_nomes = df_nomes.withColumn("AnoNascimento", indices_anos)

df_nomes.show()

# Etapa 6 - Selecionando Pessoas que nasceram no século XXI
df_select = df_nomes.select("*").filter(df_nomes.AnoNascimento > 2000)
df_select.show(10)

# Etapa 7 - Repetir o processo com Spark SQL
df_nomes.createOrReplaceTempView("pessoas")

spark.sql("SELECT * FROM pessoas WHERE AnoNascimento >= 2000").show(10)

# Etapa 8 - Contando pessoas que são da geração Millenials
df_select = df_nomes.filter(df_nomes.AnoNascimento.between(1980, 1994)).agg(count("*"))
df_select.show()

# Etapa 9 - Repetindo o processo com SQL
spark.sql("SELECT COUNT(*) AS Millenials FROM pessoas WHERE AnoNascimento BETWEEN 1980 AND 1994").show()

# Etapa 10 - Obtendo a quantidade de pessoas por país para cada geração usando SQL
df_resultado = spark.sql("""SELECT Pais, CASE 
            WHEN AnoNascimento BETWEEN 1945 AND 1964 THEN 'Baby Boomers' 
            WHEN AnoNascimento BETWEEN 1965 AND 1979 THEN 'Geração X'
            WHEN AnoNascimento BETWEEN 1980 AND 1994 THEN 'Millenials - Y'
            WHEN AnoNascimento BETWEEN 1995 AND 2015 THEN 'Geração Z'
            ELSE 'Outras'
            END AS Geracoes,
            COUNT(*) AS Quantidade
            FROM pessoas
            GROUP BY Pais, Geracoes
            ORDER BY Pais ASC, Geracoes ASC, Quantidade ASC """)
df_resultado.show(100, truncate=False)