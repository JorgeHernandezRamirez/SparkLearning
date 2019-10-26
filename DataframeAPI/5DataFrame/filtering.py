from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, column, lit, avg

if __name__ == '__main__':
    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()
    df = spark.read.format('csv')\
                   .option('sep', ';')\
                   .option('header', 'true')\
                   .load('user.csv')

    #df.filter y df.where es lo MISMO
    # - Expresion string
    # - Expresion columnar


    df.where("age > 30 and age < 35").show()
    df.where(col("age") > 30).where(col("age") < 35).show()
    df.where(expr("age") > 30).where(expr("age < 35")).show()
    df.where((col("age") > 30) & (expr("age < 35"))).show() #Las dos expresiones del and se ejecutan al mismo tiempo. No hay garantia del orden

    df.where(df.age > 30).show()