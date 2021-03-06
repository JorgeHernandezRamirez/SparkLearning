from pyspark.sql import SparkSession
from pyspark.sql.functions import count, collect_set, col

if __name__ == '__main__':
    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()
    df = spark.read.format('csv')\
                   .option('sep', ';')\
                   .option('header', 'true')\
                   .load('user.csv')

    print(df.count()) #action
    df.select(count('age')).show()

    df.agg(collect_set('age')).show()
    df.select(collect_set(col('age'))).show()
    df.selectExpr("collect_set(age)").show()

    df.drop()

