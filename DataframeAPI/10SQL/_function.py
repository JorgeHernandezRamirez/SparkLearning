from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()

    spark.udf.register('squared', lambda x: x*x)

    spark.sql("show functions").show()
    spark.sql("show user functions").show()
    spark.sql("show system functions").show()
    spark.sql("show system functions '*q'").show()
