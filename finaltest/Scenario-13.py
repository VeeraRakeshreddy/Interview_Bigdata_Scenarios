import os
import sys
from itertools import count

python_path = sys.executable
os.environ['PYSPARK_PYTHON']= python_path
os.environ['JAVA_HOME']=r'C:\Users\veera\.jdks\ms-17.0.15'

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("DepartmentData").getOrCreate()

data = [
    (1, "Jhon", "Development"),
    (2, "Tim", "Development"),
    (3, "David", "Testing"),
    (4, "Sam", "Testing"),
    (5, "Green", "Testing"),
    (6, "Miller", "Production"),
    (7, "Brevis", "Production"),
    (8, "Warner", "Production"),
    (9, "Salt", "Production")
]

schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("emp_name", StringType(), True),
    StructField("dept", StringType(), True)
])

df = spark.createDataFrame(data, schema)
df.show()

resultdf = df.groupby('dept').agg(count['emp_name'].alias("count")).show()