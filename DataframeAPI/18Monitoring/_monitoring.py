import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, avg, min, max, count

if __name__ == '__main__':

    class HybridPandasSerie:

        def __init__(self, serie):
            self.serie = serie

        def __mul__(self, other):
            self.serie = self.serie * other
            return self

        def __add__(self, other):
            if isinstance(other, HybridPandasSerie):
                other = other.serie
            self.serie = self.serie + other
            return self

        def __gt__(self, other):
            if isinstance(other, HybridPandasSerie):
                other = other.serie
            self.serie = self.serie > other
            return self

        def __str__(self):
            return str(self.serie)

        def max(self):
            return self.serie.max()

    class HybridPandasDataFrame:

        def __init__(self, df):
            self.df = df

        def __getitem__(self, other):
            #Si es una columna utilizar DataFrame
            if isinstance(other, list):
                return HybridPandasDataFrame(df[other])
            if isinstance(other, HybridPandasSerie):
                return HybridPandasDataFrame(df[other.serie])
            return HybridPandasSerie(df[other])

        def __str__(self):
            return str(self.df)

    df = pd.DataFrame([[1, 2], [2, 2], [3, 3]], columns=['A', 'B'])
    df2 = HybridPandasDataFrame(df)
    print(df2['A'] + 2)
    print(df2[['A']])
    print(df2[df2['A'] > 1])
    print((df2['A'] + df2['B']) * 2)
    print(df2['A'].max())

    class A:

        def __init__(self, value):
            self.value = value

        def __add__(self, other):
            self.value = self.value + other
            return self

        def __mul__(self, other):
            self.value = self.value * other
            return self

        def __str__(self):
            return str(self.value)


    print((A(1) + 1) * 2)


    spark = SparkSession.builder.master("local").getOrCreate()
    df = spark.read.format('csv')\
                  .option('header', True)\
                  .option('sep', ';')\
                  .schema("id INT, name STRING, surname STRING, age INT ")\
                  .load('user.csv')

    import databricks.koalas as ks
    df = ks.read_csv('user.csv', sep=";", header=0)
    print(df[df['age'] > 30])

    (df.to_spark())
    df.toPandas()

    #Pandas to koalas
    ks.from_pandas(df)
    #Spark to koalas
    ks.DataFrame(df.to_spark())


    from pyspark.sql.types import *
    schema = StructType(
        [StructField('id', IntegerType()), StructField('name', StringType()), StructField('surname', StringType()),
         StructField('age', IntegerType(), False)])
    df = spark.read.format('csv').option('header', 'True').option('sep', ';').schema(schema).load(
        'user.csv')
    df.show()

    df.groupby('surname').pivot('name').agg(avg(col('age'))).show()

    df.filter(col('surname').isNull()).show()
    df_aux = df.filter(col('age') < 30)

    df_aux.cache()
    df_aux.count()

    df_aux.show()

    miwindow = Window.partitionBy('surname').orderBy(col('age').asc()).rowsBetween(Window.unboundedPreceding, Window.currentRow )
    df.select("name", "surname", min(col('age')).over(miwindow).alias('min_age')).show()


    b = spark.createDataFrame([[1002], [3001], [4002], [2003], [2002], [3004], [1003], [4006]], ['id']).\
            withColumn("x", col("id") % 1000)

    c = b.groupBy(col("x"))\
    .agg(count("x"), sum("value"))\
    .drop("x")\
    .toDF("count", "total")\
    .orderBy(col("count").desc, col("total"))\
    .limit(1)\
    .show()

