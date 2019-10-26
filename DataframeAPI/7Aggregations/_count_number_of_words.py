from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, explode, count, max

if __name__ == '__main__':
    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()
    row = spark.read.text('README.md')\
                    .select(split(col('value'), " ").alias('word'))\
                    .select(explode(col('word')).alias('word'))\
                    .groupBy('word').agg(count(col('word')).alias('counter'))\
                    .sort(col('counter').desc())\
                    .limit(1)\
                    .take(1)
    print(row[0].word)

    df = spark.read.text('README.md') \
        .select(split(col('value'), " ").alias('word')) \
        .select(explode(col('word')).alias('word'))
    df.groupBy('word').agg(count(col('word')).alias('counter')).show()
    df.sort(col('word').desc()).show()

    df.sort(col('word').desc()).explain()
    df.groupBy('word').agg(count(col('word')).alias('counter')).explain()

    #Version sql
    spark.read.text('README.md').registerTempTable('text')
    row = spark.sql("""
            
            select word, counter
            from(select word, count(*) as counter
                 from (select explode(split(value, " ")) as word
                       from   text)
            group by word)
            order by counter desc
            limit(1)
    """).take(1)
    print(row[0].word)







