from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col

if __name__ == '__main__':
    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()
    spark.createDataFrame([[1, "Jorge", "Hernandez"]], schema="id INT, name STRING, surname STRING").registerTempTable("user")
    
    
    spark.sql("select * from user").show()
    spark.sql("show tables").show()

    df = spark.createDataFrame([[1, "Jorge", "Hernandez"]], schema="id INT, name STRING, surname STRING")
    df.select("*").show()
    df.select(expr("*")).show()
    df.select(col("*")).show()

    spark.sql("show tables").show()
    spark.sql("""create table if not exists users(id int comment 'id del usuario', name string comment 'nombre del usuario')
               using json options(path 'user.csv')""" )
    spark.sql("show tables").show()
    spark.stop()

    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()
    spark.sql("show tables").show()
