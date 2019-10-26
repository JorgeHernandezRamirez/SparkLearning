from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()
    #Unmanaged table. Borrar el fichero spark-warehouse que se crea
    df = spark.createDataFrame([[1, "Jorge", "Hernandez"]], schema="id INT, name STRING, surname STRING")
    df.repartition(5).write.mode('overwrite').saveAsTable('user')
    spark.sql('drop table user');

    #Manage table. Borrar el fichero spark-warehouse que se crea pero no borrar el fichero original
    spark.read.csv('user.csv').write.mode('overwrite').saveAsTable('user')
    spark.sql('drop table user');

    timestampFormat = "EEE MMM dd HH:mm:ss ZZZZZ yyyy"

    urlWithTimestampDF = (urlDF
                          .withColumn("timestamp",
                                      unix_timestamp("created_at", timestampFormat).cast(TimestampType()).alias(
                                          "createdAt"))
                          .drop("created_at")
                          .withColumn("hour", hour("timestamp"))
                          )

    display(urlWithTimestampDF)

    df.persist