from pyspark import SparkContext, SparkConf, Row
from pyspark.sql import SparkSession
from timeit import default_timer as timer

if __name__ == '__main__':

    def execute(spark_sql_shuffle_partitions = 200):
        sc = SparkContext(conf=SparkConf().setAppName("sample").setMaster("local").set("spark.sql.shuffle.partitions", spark_sql_shuffle_partitions))
        spark = SparkSession(sc)
        employee = sc.parallelize([Row(name="Bob"), Row(name="Alice")]).toDF()  # To DF solo está si creamos un Spark Sesion
        department = sc.parallelize([Row(name="Bob", department="Accounts", age="30"), Row(name="Alice", department="Sales", age="20")]).toDF()
        start = timer()

        employee.repartition(5).where("name = 'Bob'").explain()

        employee.join(department, "name").show()
        print("spark_sql_shuffle_partitions = {}, tiempo = {}".format(spark_sql_shuffle_partitions, timer() - start))
        sc.stop()

    execute(200)
    execute(2)