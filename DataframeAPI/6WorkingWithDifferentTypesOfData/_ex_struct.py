from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, split, explode, expr, create_map

if __name__ == '__main__':
    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()
    df = spark.read.format('csv')\
                   .option('sep', ';')\
                   .option('header', 'true')\
                   .load('user.csv')

    df1 = df.select(struct(col('age'), col('name')).alias('mistruct'))
    df1.select(col('mistruct').getField("age")).show()
    df1.selectExpr("mistruct.age").show()
    df1.select("mistruct.age").show()
    print(df1.columns)

    df.select(split(col('profesion'), " ").alias('miarray'))\
      .selectExpr("miarray[1]").show()
    df.select(split(col('profesion'), " ").alias('miarray'), expr("*")) \
      .select(explode(col("miarray")).alias('explode'), expr("*")).show()

    df.select(create_map(col("name"), col("age")).alias('mimapa')).show()
    df.select(create_map(col("name"), col("age")).alias('mimapa'))\
      .select(expr("mimapa['Jorge']")).show()
    df.select(create_map(col("name"), col("age")).alias('mimapa'), expr("*")) \
      .select(explode("mimapa"), col("*")).show()





