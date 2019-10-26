from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()
    df = spark.read.format('csv')\
                   .option('sep', ';')\
                   .option('header', 'true')\
                   .load('user.csv')

    """dfs = df.randomSplit([0.25, 0.75], seed=None)
    dfs[0].show()
    dfs[1].show()"""
    df.sample(withReplacement=False, fraction=0.75, seed=None).show()
    df.sample(withReplacement=True, fraction=0.75, seed=None).show()