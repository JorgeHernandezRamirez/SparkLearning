from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, column, lit, avg

if __name__ == '__main__':
    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()
    df = spark.read.format('csv')\
                   .option('sep', ';')\
                   .option('header', 'true')\
                   .load('user.csv')

    print(df.take(5))
    df.foreach(lambda person: print(person))
    [print(person) for person in df.toLocalIterator()]