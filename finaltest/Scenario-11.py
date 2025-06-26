import os
import sys

from pyspark.sql.functions import explode

python_path = sys.executable
os.environ['PYSPARK_PYTHON']= python_path
os.environ['JAVA_HOME']=r'C:\Users\veera\.jdks\ms-17.0.15'

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import *

# Start Spark session
spark = SparkSession.builder.appName("EmployeeData").getOrCreate()

# Data
data = [
    (1, "Jhon", 4000),
    (2, "Tim David", 12000),
    (3, "Json Bhrendroff", 7000),
    (4, "Jordon", 8000),
    (5, "Green", 14000),
    (6, "Brewis", 6000)
]

# Schema
schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("emp_name", StringType(), True),
    StructField("salary", IntegerType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)
df.show()

# USing DSL
result = df.withColumn("Grade",expr("CASE WHEN salary >10000 THEN 'A' WHEN salary < 5000 THEN 'C' else 'B' end"))

result.show()

#Using SQL
data = df.createOrReplaceTempView("mydata")

sqlquery = spark.sql("SELECT *, CASE WHEN salary >10000 THEN 'A' WHEN salary < 5000 THEN 'C' else 'B' end as Grade from mydata")
sqlquery.show()