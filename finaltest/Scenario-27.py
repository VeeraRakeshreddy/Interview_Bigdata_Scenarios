import os
import sys


python_path = sys.executable
os.environ['PYSPARK_PYTHON']= python_path
os.environ['JAVA_HOME']=r'C:\Users\veera\.jdks\ms-17.0.15'

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *

spark = SparkSession.builder.appName("EmployeeSalary").getOrCreate()

data = [
    (1, 60000, 2018),
    (1, 70000, 2019),
    (1, 80000, 2020),
    (2, 60000, 2018),
    (2, 65000, 2019),
    (2, 65000, 2020),
    (3, 60000, 2018),
    (3, 65000, 2019)
]

columns = ["empid", "salary", "year"]

df = spark.createDataFrame(data, schema=columns)
df.show()

windowspec = Window.partitionBy("empid").orderBy("year")

salarydiff = df.withColumn("newSalary",lag("salary",1).over(windowspec))
salarydiff.show()

increasedSalary = salarydiff.withColumn("increasedSalary",expr("salary-newSalary")).drop("newSalary").fillna(0)
increasedSalary.show()