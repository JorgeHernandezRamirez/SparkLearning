from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce

if __name__ == '__main__':
    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()
    df = spark.read.format('csv')\
                   .option('sep', ';')\
                   .option('header', 'true')\
                   .load('null.csv')

    df.select(coalesce(col("age"), col("age_son")).alias("age")).show()

    df.na.replace()



