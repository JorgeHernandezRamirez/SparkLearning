from array import ArrayType

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, count, mean, stddev_pop, col, count, expr, column, lit, avg, \
    monotonically_increasing_id, round, bround, rand, corr, instr

if __name__ == '__main__':

    df = pd.DataFrame([[1, 2]], columns=['A', 'B'])
    df[df['A'] > 1]

    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()
    df = spark.read.format('csv')\
                   .option('sep', ';')\
                   .option('header', 'true')\
                   .load('user.csv')

    df.repartition(5).select(max('id')).show()
    df.repartition(5).select(avg('age')).show()

    df.select(round(lit(2.5)), bround(lit(2.5))).show()
    df.select(round(lit("2.5")), bround(lit("2.5"))).show()


    spark.range(0, 10, 2)\
         .select(monotonically_increasing_id().alias('id_v2'),
                round((rand() * 5 + 5)).alias('rand')).show()

    spark.range(0, 10, 2)\
         .select(monotonically_increasing_id().alias('id'),
                 pow(col('id'), 2).alias('pow')).show()

    df.describe().show()

    df.select(count('age'),
              mean('age'),
              min('age'),
              max('age'),
              stddev_pop('age')).show()

    df.select(corr(col('age'), col('name'))).show()
    df.stat.corr(col('age'), col('name')).show()
