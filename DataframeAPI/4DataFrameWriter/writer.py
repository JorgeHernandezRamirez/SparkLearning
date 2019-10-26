from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()

    df = spark.createDataFrame([[1, 'Jorge'], [3, 'Jose'], [0, 'Barbara'], [2, 'Juan'], [7, 'Pepe'], [8, 'Pepe Luis']], "id INT, name STRING")

    df.write.json
    df.write.csv
    df.write.jdbc
    df.write.orc
    df.write.parquet
    df.write.text
    df.write.saveAsTable
    #Otros
    df.write.mode
    df.write.option
    df.write.save
    #bucketBy, partitionBy (saveAsTable, orc, parquet), sortBy
    df.write.bucketBy
    df.write.partitionBy
    df.write.sortBy

    #3 modos de overrite
    df.write.mode('overwrite').csv('/tmp/1.csv')
    df.write.csv('/tmp/1.csv', mode="overwrite")
    df.write.format('csv').save('/tmp/2.csv', mode="overwrite")

    #partitionBy
    df.write.partitionBy("name").format("parquet").save("user.partition", mode="overwrite")

    #sortBy + bucketBy + saveAsTable -> Solo puede ser utilizado sortBy con el bucketby
    df.write.bucketBy(4, "name").sortBy("id").format("csv").saveAsTable("user", mode="overwrite")
    df = spark.sql("SELECT * FROM user")
    print(df.rdd.getNumPartitions())

    df.write.partitionBy("name").bucketBy(4, "name").sortBy("id").format("parquet").saveAsTable("user.partition.bucketby", mode="overwrite")








