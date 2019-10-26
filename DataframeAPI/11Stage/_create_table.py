from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, max

if __name__ == '__main__':
    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()
    #Only 1 stage
    df = spark.range(0, 100000)
    df = df.filter(col("id") > 1)
    df = df.filter(col("id") < 1000)
    df = df.select(max("id"))
    df.explain()
    df.show()
    print(1)

    #2 stage
    df = spark.range(0, 100000).sort(col("id").desc())
    df = df.filter(col("id") > 1)
    df = df.filter(col("id") < 1000)
    df = df.select(max("id"))
    df.explain()
    df.show()
    print(1)
