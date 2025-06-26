import os
import sys

from pyspark.sql.functions import explode

python_path = sys.executable
os.environ['PYSPARK_PYTHON']= python_path
os.environ['JAVA_HOME']=r'C:\Users\veera\.jdks\ms-17.0.15'

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
from pyspark.sql.functions import *

# Start Spark session
spark = SparkSession.builder.appName("RankData").getOrCreate()

# Define data
data = [
    ("a", [1, 1, 1, 3]),
    ("b", [1, 2, 3, 4]),
    ("c", [1, 1, 1, 1, 4]),
    ("d", [3])
]

# Define schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("rank", ArrayType(IntegerType()), True)
])



# Create DataFrame
df = spark.createDataFrame(data, schema)
df.show()

explodedf = (df
             .withColumn("rank",explode('rank'))
             .filter(col('rank')==1)
             .groupby("name")
             .agg(count(col('rank')).alias('count'))
             .orderBy(col('count').desc())
             .select("name")
             )
explodedf.show()

firstdf = print(explodedf.first()['name'])
