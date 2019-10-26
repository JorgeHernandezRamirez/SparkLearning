from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()
    df = spark.read.format('csv').option('sep', ';').option('header', 'true').load('../../resources/folder')
    #https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-Dataset-basic-actions.html
    """df.collect
    df.count
    df.take
    df.limit #No es una accion pq devuelve un df
    df.foreach
    df.foreachPartition
    df.coalesce
    df.write
    df.writeStream
    df.cache
    df.checkpoint
    df.hint"""

    df = spark.range(0, 100, 2)
    print(df.rdd.getNumPartitions())

    df.write.sortBy("id").bucketBy(4, "id").format("csv").saveAsTable("mytable", mode="overwrite")
    df = spark.table("mytable")
    print(df.rdd.getNumPartitions())

    df = df.coalesce(2)
    print(df.rdd.getNumPartitions())