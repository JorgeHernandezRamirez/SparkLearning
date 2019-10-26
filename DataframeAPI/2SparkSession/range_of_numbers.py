import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, rand, round
from pyspark.sql.types import StructType, StructField, FloatType

if __name__ == '__main__':

    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()

    spark.range(1, 20, 2).show()

    #Inicializando random values
    random_values = pd.np.random.rand(100, 3)
    df = spark.createDataFrame(random_values.tolist(), schema=StructType([StructField("a", FloatType()), StructField("b", FloatType()), StructField("c", FloatType())]))

    #Creando columna 1 o 0 random
    df = df.withColumn('randombool', when(rand() > 0.5, 1).otherwise(0))

    #Creando columna random numerica entre 0 y 1
    df = df.withColumn('randomvalue', rand())

    #Creando columna numerico entre 5 y 10
    df = df.withColumn("randomvalue510", round(rand() * (10 - 5) + 5, 0))
    df.show()