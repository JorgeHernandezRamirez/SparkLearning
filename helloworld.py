from pyspark import SparkContext
from pyspark.examples.src.main.python.status_api_demo import delayed
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import countDistinct

if __name__ == '__main__':
    SPARK_SESSION_APP_NAME = "testing_spark"
    SPARK_MASTER_URL = "local"

    spark = SparkSession.builder.appName(SPARK_SESSION_APP_NAME).master(SPARK_MASTER_URL).getOrCreate()
    print(spark)
    df = spark.createDataFrame([['jorge', 31], ['jose', 32]],
                               schema=StructType([StructField("name", StringType()),
                                                  StructField("age", IntegerType())]))
    df.select(countDistinct(df['name']).alias('counter')).show()

    df1 = spark.createDataFrame([['jorge', 31], ['jose', 32]],
                               schema="name STRING, age INT").show()

    spark.range()


    SparkContext


    def ejemplo2():
        sc = SparkContext(master="local", appName="learning")
        rdd = sc.parallelize(range(10), 10).map(delayed(2))
        reduced = rdd.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
        print(reduced.map(delayed(2)).collect())