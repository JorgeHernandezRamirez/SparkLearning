from pyspark.sql import SparkSession

if __name__ == '__main__':

    #https://spark.apache.org/docs/latest/sql-data-sources.html

    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()

    spark.read.json
    spark.read.csv
    spark.read.jdbc
    spark.read.orc
    spark.read.parquet
    spark.read.text
    spark.read.table
    #El generico
    spark.read.load

    #Table
    spark.range(0, 10, 2).registerTempTable("test")
    print(spark.read.table('test').collect())

    #Text
    print(spark.read.text("../../resources/text.txt", wholetext=False).collect())
    print(spark.read.text("../../resources/text.txt", wholetext=True).collect())

    #Parquet
    #spark.read.parquet("../../resources/user.parquet").show()

    #Orc
    #spark.read.orc(path)

    #Jdbc
    # spark.read.jdbc(path)

    #Csv
    spark.read.csv("../../resources/folder", sep=";", header="True").show()

    spark.read.format('csv').option('sep', ';').load('../../resources/folder/country=EN/user.csv').show()
    spark.read.format('csv').option('sep', ';').csv('../../resources/folder/country=EN/user.csv').show()





