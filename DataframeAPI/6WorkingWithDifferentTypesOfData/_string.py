from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, column, lit, avg, monotonically_increasing_id, rand, locate, instr

if __name__ == '__main__':
    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()
    df = spark.read.format('csv')\
                   .option('sep', ';')\
                   .option('header', 'true')\
                   .load('user.csv')

    df.select(instr(col('name'), 'Jorge'), locate('Jorge', col('name'))).show()
    df.select(rand().alias("random")).where(expr("random > 0")).show()



