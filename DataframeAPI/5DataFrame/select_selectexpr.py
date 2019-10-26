from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, column, lit, avg

if __name__ == '__main__':
    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()
    df = spark.read.format('csv')\
                   .option('sep', ';')\
                   .option('header', 'true')\
                   .load('user.csv')



    df.select('id', 'name').show()
    df.select(col('id'),
              expr('name'),
              column('name')).show()
    df.select(col('id'), 'name').show() #El libro dice que esto no se puede. Veo que si
    df.select(expr('id as id2').alias("id")).show()
    df.select(expr('*')).show()
    df.select('*').show()
    df.select(expr('sum(id)').alias("sumid"), expr('avg(age) < 30 as young')).show()
    df.selectExpr("*", "(age < 30) as young").show()

    df.select((col('age') - 5) * 2 < col("age"))
    df.select(expr('(age - 5) * 2 < age'))

    #Literal
    df.select(expr("*"), lit(1).alias('One')).show()

    #Columna con espacios
    df.select(expr("`columna con espacios`")) #Tenemos que escapar con `
    print(df.withColumnRenamed('columna con espacios', 'columna_con_espacios').columns) #No hace falta escapar

    #Creando columnas
    df = df.withColumn('field1', lit(1)) #Si no hay una accion no se crea la co
    df = df.withColumn('field2', expr("field1"))
    df = df.withColumn('field3', column("field2"))

    df = df.select(avg('age').alias('avg_alias'))

    #Spark is insensitive
    df.select(expr("avg_alias"), 'AVG_ALIAS').show()
    spark.stop()

    #Set caseSesitive = true
    spark = SparkSession.builder.appName("learning") \
        .master("local") \
        .getOrCreate()
    spark.sql("set spark.sql.caseSensitive=true")
    df = spark.read.format('csv') \
        .option('sep', ';') \
        .option('header', 'true') \
        .load('user.csv')
    df.select(col('age'), col('AGE')).show()  # La columna no existe AGE







