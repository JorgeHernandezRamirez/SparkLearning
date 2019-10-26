from audioop import avg

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, max, avg

if __name__ == '__main__':
    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()
    df = spark.read.format('csv').option('header', True).option('sep', ';').option('path', 'user.csv').load()
    df.sample(False, 0.25, 3).show()
    df.where(col('id') > 1).select(expr("name as nombre")).show()
    print(df.first())
    df.cube(['name', 'apellido']).agg(max('age')).show()
    df.groupBy('apellido').pivot('name').agg(avg(col('age')))
    df = df.withColumnRenamed('apellido', 'surname')
    df.orderBy(col('id').desc())
