from pyspark.sql import SparkSession
from pyspark.sql.functions import count, collect_set, col

if __name__ == '__main__':
    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()
    df = spark.read.format('csv')\
                   .option('sep', ';')\
                   .option('header', 'true')\
                   .load('user.csv')

    df.registerTempTable("user")

    spark.sql("""
        select name, avg(age) as avg_age
        from   user
        group by name
    """).show()

    spark.sql("""
            select name, avg(age) as avg_age
            from   user
            group by name grouping sets((name))
        """).show()

    spark.sql("""
                select name, apellido, avg(age) as avg_age
                from   user
                group by name, apellido grouping sets((name, apellido), (apellido), ())
            """).show()

    #Los valores en el grouping set deben estar en el groupby!
