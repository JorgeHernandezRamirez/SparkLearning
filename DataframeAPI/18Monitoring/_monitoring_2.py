from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, avg, min, max

if __name__ == '__main__':
    spark = SparkSession.builder.master("local").getOrCreate()
    spark.read.format('csv')\
                .option('header', True)\
                .option('sep', ';')\
                .schema("id INT, name STRING, surname STRING, age INT ")\
                .load('user.csv')\
                .repartition(3)\
                .filter(col('age') < 30)\
                .groupby('surname')\
                .count()\
                .show()
    print('hola')
