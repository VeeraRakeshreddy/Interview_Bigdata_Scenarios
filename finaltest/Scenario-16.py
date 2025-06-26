import os
import sys


python_path = sys.executable
os.environ['PYSPARK_PYTHON']= python_path
os.environ['JAVA_HOME']=r'C:\Users\veera\.jdks\ms-17.0.15'

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import *

# Create Spark session
spark = SparkSession.builder.appName("EmployeeDeptSalary").getOrCreate()

# Define data
data = [
    (1, "Jhon", "Testing", 5000),
    (2, "Tim", "Development", 6000),
    (3, "Jhon", "Development", 5000),
    (4, "Sky", "Prodcution", 8000)
]

# Define schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("dept", StringType(), True),
    StructField("salary", IntegerType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)
df.show()

dropdup = df.dropDuplicates(['name']).show()