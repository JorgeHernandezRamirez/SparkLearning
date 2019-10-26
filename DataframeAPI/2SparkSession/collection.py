# import pyspark class Row from module sql
import os
from pyspark.sql import *

if __name__ == '__main__':

    def removeIfExists(path):
        if os.path.exists(path):
            os.remove(path)

    # Create Example Data - Departments and Employees
    spark = SparkSession.builder.appName("learning").master("local").getOrCreate()

    # Create the Departments
    department1 = Row(id='123456', name='Computer Science')
    department2 = Row(id='789012', name='Mechanical Engineering')
    department3 = Row(id='345678', name='Theater and Drama')
    department4 = Row(id='901234', name='Indoor Recreation')

    # Create the Employees
    Employee = Row("firstName", "lastName", "email", "salary")
    employee1 = Employee('michael', 'armbrust', 'no-reply@berkeley.edu', 100000)
    employee2 = Employee('xiangrui', 'meng', 'no-reply@stanford.edu', 120000)
    employee3 = Employee('matei', None, 'no-reply@waterloo.edu', 140000)
    employee4 = Employee(None, 'wendell', 'no-reply@berkeley.edu', 160000)

    # Create the DepartmentWithEmployees instances from Departments and Employees
    departmentWithEmployees1 = Row(department=department1, employees=[employee1, employee2])
    departmentWithEmployees2 = Row(department=department2, employees=[employee3, employee4])
    departmentWithEmployees3 = Row(department=department3, employees=[employee1, employee4])
    departmentWithEmployees4 = Row(department=department4, employees=[employee2, employee3])

    departmentsWithEmployeesSeq1 = [departmentWithEmployees1, departmentWithEmployees2]
    df1 = spark.createDataFrame(departmentsWithEmployeesSeq1)

    df1.show()

    departmentsWithEmployeesSeq2 = [departmentWithEmployees3, departmentWithEmployees4]
    df2 = spark.createDataFrame(departmentsWithEmployeesSeq2)

    df2.show()

    unionDF = df1.unionAll(df2)
    removeIfExists("~/Escritorio/databricks-df-example.parquet")
    unionDF.write.parquet("~/Escritorio/databricks-df-example.parquet")

    unionDF.write.json