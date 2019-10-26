from pyspark.sql import SparkSession
from pyspark.sql.functions import count, collect_set, col, avg

if __name__ == '__main__':
    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()
    df = spark.read.format('csv')\
                   .option('sep', ';')\
                   .option('header', 'true')\
                   .load('user.csv')

    df.cube('apellido', 'name').agg(avg('age')).show()
    #Lo mismo que el rollup para en todas las direcciones
