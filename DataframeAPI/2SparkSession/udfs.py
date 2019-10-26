from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, pandas_udf, PandasUDFType
from pyspark.sql.types import LongType, StructType, StructField, FloatType


def squared(s):
    return s * s


if __name__ == '__main__':

    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()

    #UDF in SparkSQL
    spark.udf.register("squaredWithPython", squared, LongType()) #Por defecto spark.udf.register devuelve un StringType()
    spark.range(1, 20).registerTempTable("test")
    spark.sql("SELECT id, squaredWithPython(id) from test").show()

    #UDF in Dataframes
    squared_udf = udf(squared, LongType())
    df = spark.table("test")
    df.select("id", squared_udf("id").alias("id_squared")).show()

    #UDF in Dataframes v2. Si lo ponemos junto con el squared dará un fallo ya que no está creada la sesión
    @udf("long")
    def squared_v2(s):
        return s * s
    df = spark.table("test")
    df.select("id", squared_v2("id").alias("id_squared")).show()

    #En los sql no se garantiza el orden de las sentencias entre and or
    spark.udf.register("strlen", lambda s: len(s), "int")
    spark.sql("select id from test where id is not null and strlen(id) > 1")
    #No se garantiza que la segunda parte se ejecute a continuación de la primera. Para solucionarlo la lambda debería controlar
    spark.udf.register("strlen_nullsafe", lambda s: len(s) if not s is None else -1, "int")
    spark.sql("select id from test where id is not null and strlen_nullsafe(id) > 1")
    spark.sql("select id from test where if(id is not null, strlen(id), null) > 1")

    #Devolvemos tipos complejos
    schema = StructType([
        StructField('sum', FloatType()),
        StructField('mult', FloatType()),
        StructField('div', FloatType())
    ])
    def mathematical(x, y):
        return (float(x + y), float(x * y), float(x / y))

    df = spark.createDataFrame([[1, 2], [2, 3]], ['A', 'B'])
    mathematical_udf = spark.udf.register('mathematical', mathematical, schema)
    df.select(mathematical_udf(col('A'), col('B')).alias('mathematical')).show()


    #Pandas
    """@pandas_udf('double', PandasUDFType.SCALAR)
    def pandas_plus_one(v):
        return v + 1


    df = spark.range(0, 10 * 1000 * 1000)
    df.withColumn('id_transformed', pandas_plus_one("id")).show()"""
