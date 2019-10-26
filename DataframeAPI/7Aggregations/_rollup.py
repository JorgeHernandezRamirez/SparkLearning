from pyspark.sql import SparkSession
from pyspark.sql.functions import count, collect_set, col, avg

if __name__ == '__main__':
    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()
    df = spark.read.format('csv')\
                   .option('sep', ';')\
                   .option('header', 'true')\
                   .load('user.csv')

    df.rollup('apellido', 'name').agg(avg('age')).show()


    #Los valores en el grouping set deben estar en el groupby!
