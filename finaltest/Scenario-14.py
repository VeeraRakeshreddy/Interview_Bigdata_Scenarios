import os
import sys
from itertools import count

python_path = sys.executable
os.environ['PYSPARK_PYTHON']= python_path
os.environ['JAVA_HOME']=r'C:\Users\veera\.jdks\ms-17.0.15'

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import *

# Start Spark session
spark = SparkSession.builder.appName("StudentMarks").getOrCreate()

data = [(203040, "rajesh", 10, 20, 30, 40, 50)]

schema = StructType([
    StructField("rollno", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("telugu", IntegerType(), True),
    StructField("english", IntegerType(), True),
    StructField("maths", IntegerType(), True),
    StructField("science", IntegerType(), True),
    StructField("social", IntegerType(), True)
])

df = spark.createDataFrame(data, schema)

df.show()

totaldf = df.withColumn("total",expr("telugu + english + maths + science + social")).show()