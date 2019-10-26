from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, Row

if __name__ == '__main__':
    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()
    spark.createDataFrame([[1, "Jorge", "Hernandez"]], schema="id INT, name STRING, surname STRING").show()
    spark.createDataFrame([[1, "Jorge", "Hernandez"]], schema=StructType([StructField('id', IntegerType()),
                                                                          StructField('name', StringType()),
                                                                          StructField('surname', StringType())])).show()
    spark.createDataFrame([Row(id=1, name="Jorge", surname="Hernandez")]).show()
    spark.createDataFrame([[1, "Jorge", "Hernandez"]]).toDF("id", "name", "surname").show()