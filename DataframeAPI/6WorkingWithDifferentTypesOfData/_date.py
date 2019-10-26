from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date, current_date, \
    current_timestamp, date_add, date_sub, datediff, months_between, to_timestamp, hour

if __name__ == '__main__':
    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()

    spark.range(5).withColumn('date', to_date(lit('2019-01-01'))).show()

    spark.read.jdbc

    spark.range(5)\
         .select(current_date().alias('date'), current_timestamp().alias('timestamp'))\
         .select(date_add(col('date'), 1), date_sub(col('timestamp'), 1)).show()

    spark.range(5).select(to_date(lit('2019-01-01')).alias('date1'),
                          to_date(lit('2019-01-05')).alias('date2'))\
                  .select(datediff(col('date2'), col('date1')),
                          months_between(col('date2'), col('date1'))).show()

    spark.range(5).withColumn('date', to_date(lit('2019-XX-XX'))).show() #No emite excepcion

    spark.range(5).withColumn('date_comp1', to_date(lit('2019-01-01')) > to_date(lit('2019-01-02'))) \
                  .withColumn('date_comp2', to_date(lit('2019-01-01')) > to_timestamp(lit('2019-01-02'))) \
        .withColumn('date_comp3', to_date(lit('2019-01-01')) > "2019-01-02") \
                  .withColumn('date_comp3', to_date(lit('2019-01-01')) > "'2019-01-02'").show()

    spark.range(5).select(current_timestamp().alias("timestamp")).select(hour(col('date')))


