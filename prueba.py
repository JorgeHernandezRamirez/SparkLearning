from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()
    spark.range(0, 10, 2).show()