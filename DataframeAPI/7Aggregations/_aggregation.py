from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import count, collect_set, col, monotonically_increasing_id, max, rank, dense_rank

if __name__ == '__main__':
    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()
    df = spark.read.format('csv')\
                   .option('sep', ';')\
                   .option('header', 'true')\
                   .schema('id int, name string')\
                   .load('user.csv')

    spark.createDataFrame([[1, 2], [2, 3]], ['id', 'name']).show()
    df.rdd

    print(df.count()) #action
    df.select(count('age')).distinct()

    spark.createDataFrame

    df.agg(collect_set('age')).show()
    df.select(collect_set(col('age'))).show()
    df.selectExpr("collect_set(age)").show()

    df.drop()

    df = df.withColumn("id", monotonically_increasing_id())
    df.show()

    window = Window.partitionBy("apellido").orderBy(col("age").desc())
    df.select("name",
              "apellido",
              max(col('age')).over(window).alias("age"),
              rank().over(window).alias("rank"),
              dense_rank().over(window).alias("dense_rank")).show()



    # ANSWER
    from pyspark.sql.functions import unix_timestamp
    from pyspark.sql.types import TimestampType

    timestampFormat = "EEE MMM dd HH:mm:ss ZZZZZ yyyy"

    tweetDF = fullTweetFilteredDF.select(col("id").alias("tweetID"),
      col("user.id").alias("userID"),
      col("lang").alias("language"),
      col("text"),
      unix_timestamp("created_at", timestampFormat).cast(TimestampType()).alias("createdAt")
    )

    df.persist(StorageLevel.M)

    display(tweetDF)
