from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, monotonically_increasing_id, \
    struct, create_map, split

if __name__ == '__main__':
    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()

    df = spark.range(10)\
              .select(monotonically_increasing_id().alias('id'), col('id').alias('age'), lit('jorge hernandez').alias('name'))\
              .select('*', struct(col('age'), col('name')).alias('struct'), create_map(col('name'), col('age')).alias('map'), split('name', ' ').alias('array'))

    df.columns
    df.write.format('parquet').mode('overwrite').save('/Users/jorgehernandezramirez/Desktop/user.parquet')
    df.repartition(5).select("age", "name").write.format('csv').mode('overwrite').save('/Users/jorgehernandezramirez/Desktop/user.csv')
    df.write.format('json').mode('overwrite').save('/Users/jorgehernandezramirez/Desktop/user.json')
