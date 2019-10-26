from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == '__main__':
    spark = SparkSession.builder.appName("learning")\
                                .master("local")\
                                .config("spark.sql.shuffle.partitions", 2) \
                                .config("spark.sql.crossJoin.enable", True).getOrCreate()
    df = spark.createDataFrame([(1, "Jorge", "Hernandez")]).toDF("id", "name", "surname")
    print(df.id)
    print(col('id'))
    print(df['id'])

    person = spark.createDataFrame([('1', 'Jorge'),
                                    ('2', 'Jose'),
                                    ('3', 'Barbara')]).toDF('id', 'name')

    profession = spark.createDataFrame([
                                    ('2', 'Licenciatura Medicina'),
                                    ('3', 'Graduada Derecho'),
                                    ('4', 'Licenciado Química'),]).toDF('id', 'proffesion')

    join_expression = person.id == profession.id
    person.join(profession, join_expression).explain()
    person.join(profession, join_expression).show() #Default inner
    person.join(profession, join_expression, 'inner').show()
    person.join(profession, join_expression, 'outer').show()
    person.join(profession, join_expression, 'left_outer').show()
    person.join(profession, join_expression, 'right_outer').show()

    person.join(profession, join_expression, 'left_semi').show()
    person.join(profession, join_expression, 'left_anti').show()
    person.join(profession, join_expression, 'cross').show() #Es como un inner

    person.crossJoin(profession).show()

    person.registerTempTable('person')
    profession.registerTempTable('profession')

    spark.sql('select * from person natural join profession').show()
    spark.sql('select * from person natural right outer join profession').show()

    spark.sql("select avg(id) from person").show()
    spark.sql("select mean(id) from person").show()

    spark.read.format().option().schema().load()

    person.write.format().option().bucketBy().sortBy().partitionBy().mode().save()