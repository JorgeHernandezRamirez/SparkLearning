from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, column, lit, avg, lpad, regexp_extract, instr

if __name__ == '__main__':
    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()
    df = spark.read.format('csv')\
                   .option('sep', ';')\
                   .option('header', 'true')\
                   .load('user.csv')

    df.select(lit(5), lit(5.5), lit("5.5"), lit("hola")).show()
    print(df.select(lit(5), lit(5.5), lit("5.5"), lit("5.5"), lit("hola")).dtypes)


    df.select(lpad(lit("HELLO"), 3, " ")).show()

    df.select(col('name').isin(['jorge'])).show()
    df.select(instr(col('name'), 'Jorge')).show()
