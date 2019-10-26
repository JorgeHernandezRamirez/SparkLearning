from pyspark.sql import SparkSession
from pyspark.sql.functions import count, collect_set, col, avg, coalesce

if __name__ == '__main__':
    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()
    df = spark.read.format('csv')\
                   .option('sep', ';')\
                   .option('header', 'true')\
                   .load('user.csv')

    df.select('name', 'apellido', 'age').groupBy('apellido').pivot('name').agg(avg('age')).show()
    df.groupby('apellido').avg().show()

