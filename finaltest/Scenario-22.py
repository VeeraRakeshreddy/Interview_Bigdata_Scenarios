import os
import sys

from pyspark.sql.functions import split, explode

python_path = sys.executable
os.environ['PYSPARK_PYTHON']= python_path
os.environ['JAVA_HOME']=r'C:\Users\veera\.jdks\ms-17.0.15'

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Start Spark session
spark = SparkSession.builder.appName("ProductPrices").getOrCreate()

# Define data
data = [
    (1, "26-May", 100),
    (1, "27-May", 200),
    (1, "28-May", 300),
    (2, "29-May", 400),
    (3, "30-May", 500),
    (3, "31-May", 600)
]

# Define schema
schema = StructType([
    StructField("pid", IntegerType(), True),
    StructField("date", StringType(), True),
    StructField("price", IntegerType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)
df.show()

win = Window.partitionBy('pid').orderBy(col("price")).rowsBetween(Window.unboundedPreceding,Window.currentRow)

cummsumm = df.withColumn("cumm_Sum",sum("price").over(win)).show()