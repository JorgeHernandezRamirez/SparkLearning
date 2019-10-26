from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, count, avg

if __name__ == '__main__':
    spark = SparkSession.builder.appName("learning").config("spark.sql.shuffle.partitions", 120).master("local").getOrCreate()

    user_df = spark.createDataFrame([[1, "Jorge", "Hernandez"], [2, "Jose", "Hernandez"]], schema="id INT, name STRING, surname STRING")
    account_df = spark.createDataFrame([[1, "E000000001"]], schema="id INT, account STRING")

    #Suffle transformation
    user_df.sort(col("id").desc()).explain()
    user_df.groupBy(col("id")).agg(count(col("name"))).explain()
    user_df.join(account_df, user_df.id == account_df.id).drop(account_df.id).explain()

    #Narrow transformation
    user_df.filter(col("id") == 1).explain()
    user_df.select(max("id")).explain()
    user_df.select(avg("id")).explain()


