import os
import sys

python_path = sys.executable
os.environ['PYSPARK_PYTHON']= python_path
os.environ['JAVA_HOME']=r'C:\Users\veera\.jdks\ms-17.0.15'

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("EmployeeData").getOrCreate()

data = [
    (1, 'a', 10000),
    (2, 'b', 5000),
    (3, 'c', 15000),
    (4, 'd', 25000),
    (5, 'e', 50000),
    (6, 'f', 7000)
]

schema = ["empid","name", "salary"]

df = spark.createDataFrame(data, schema)
df.show()

result = (df.withColumn("Designation",expr("CASE WHEN salary > 10000 THEN 'Manager' else 'Employee' end"))
          .select("empid","name","salary","Designation"))

result.show()