from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, column, lit, avg

if __name__ == '__main__':
    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()
    df = spark.read.format('csv')\
                   .option('sep', ';')\
                   .option('header', 'true')\
                   .load('user.csv')

    print(df.coalesce(5).rdd.getNumPartitions())
    print(df.repartition(5).rdd.getNumPartitions())
    print(df.repartition(2).rdd.getNumPartitions())
    print(df.repartition(col('age')).rdd.getNumPartitions())
    print(df.repartition(expr('age')).rdd.getNumPartitions())
    print(df.repartition('age').rdd.getNumPartitions())
    print(df.repartition(5, col('age')).rdd.getNumPartitions())

    print(df.repartition(5, col('age'))\
            .where(expr('age > 30'))\
            .explain())