import os
import sys

from setuptools.command.alias import alias

python_path = sys.executable
os.environ['PYSPARK_PYTHON']= python_path
os.environ['JAVA_HOME']=r'C:\Users\veera\.jdks\ms-17.0.15'

from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import *

# Start Spark session
spark = SparkSession.builder.appName("SensorData").getOrCreate()

# Define schema
schema = StructType([
    StructField("sensorid", IntegerType(), True),
    StructField("timestamp", StringType(), True),  # We'll keep it as string unless you want DateType
    StructField("values", IntegerType(), True)
])

# Define data
data = [
    (1111, "2021-01-15", 10),
    (1111, "2021-01-16", 15),
    (1111, "2021-01-17", 30),
    (1112, "2021-01-15", 10),
    (1112, "2021-01-15", 20),
    (1112, "2021-01-15", 30),
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show it
df.show()

windowspec = Window.partitionBy(col("sensorid")).orderBy(col("values").asc())


result = (df.withColumn("Next_value",lead("values",1).over(windowspec))
          .withColumn("Diff_value",expr("Next_value - values"))
          .drop("values")
          .selectExpr("sensorid","timestamp","Diff_value as values")
          .dropna(subset = ["values"])
          )
result.show()

