from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, mean, broadcast, udf, expr, col

if __name__ == '__main__':
    print(pow(2, 3))
    spark = SparkSession.builder.appName("learning")\
                                .master("local")\
                                .config("spark.sql.shuffle.partitions", 157) \
                                .config("spark.sql.crossJoin.enabled", False) \
                                .getOrCreate()

    person = spark.createDataFrame([[1, 'Jorge'], [2, 'Jose'], [3, 'Barbara']]).toDF("id", "name")
    account = spark.createDataFrame([[1, 'E0000000001'], [1, 'E0000000002'], [2, 'E0000000003']]).toDF("id", "account")

    person.crossJoin(account).show()
    person.join(account, person.id == account.id, 'cross').show()
    person.join(account, person.id == account.id, 'inner').drop(person.id).show()
    person.join(broadcast(account), 'id').explain()

    squared_udf = udf(lambda x: x*x)
    person.select("id", expr("id") + 1, squared_udf(col("id"))).show()
    person.select(col("id").isin([1, 2])).show()

    person.write.jdbc
    spark.read.jdbc


