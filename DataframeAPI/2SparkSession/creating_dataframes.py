from pyspark.sql import *
import pandas as pd

if __name__ == '__main__':
    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()

    #1
    #spark.createDataFrame([1, 2]).show() #Error

    #2. Collection
    spark.createDataFrame([[1, 2]]).show() #columns [_1, _2]

    #3. Collection Row v1
    spark.createDataFrame([Row(id=1, name='Jorge'), Row(id=2, name='Jose')]).show() #columns [id, name]

    #4. Collection Row v2
    person = Row("id", "nombre")
    spark.createDataFrame([person(1, "Jorge"), person(2, 'Jose')]).show()  # columns [id, name]

    #5. Using range
    spark.range(0, 10, 2).show() #column = [id]

    #6. From a table. Using table()
    spark.range(0, 10, 2).registerTempTable("test")
    spark.table("test").show()

    #7. From a table. Using sql()
    spark.range(0, 10, 2).registerTempTable("test")
    spark.sql("select id from test").show()

    #8. From a pandas
    pd = pd.DataFrame(pd.np.random.rand(10, 2), columns=['a', 'b']) #10 filas y dos columnas
    spark.createDataFrame(pd).show() #columns=[id, name]

    #9. From a rdd
    sc = spark.sparkContext
    rdd = sc.parallelize([Row(id=1, name='Jorge'), Row(id=2, name='Jose')])
    spark.createDataFrame(rdd).show()  #columns=[id, name]


    #--------
    #Otras interesantes

    #Spark DF to Pandas DF
    print(spark.range(0, 10, 2).toPandas())

    #Spark DF to array Rows
    print(spark.range(0, 10, 2).collect())

    #Spark set columns
    spark.createDataFrame([[1, 2]], ["a", "b"]).show()


