from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, split, explode, expr, create_map, get_json_object, json_tuple

if __name__ == '__main__':
    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()
    df = spark.range(1).selectExpr("""
        '{"myJsonKey": {"myJsonValue": [1, 2, 3]}}' as jsonString
    """)

    df.select(get_json_object(col('jsonString'), "$.myJsonKey.myJsonValue[0]")).show()
    df.select(json_tuple(col('jsonString'), "myJsonKey")).show()





