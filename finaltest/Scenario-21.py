import os
import sys

from pyspark.sql.functions import split, explode

python_path = sys.executable
os.environ['PYSPARK_PYTHON']= python_path
os.environ['JAVA_HOME']=r'C:\Users\veera\.jdks\ms-17.0.15'

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("CityDistances").getOrCreate()

data = [
    ("SEA", "SF", 300),
    ("CHI", "SEA", 2000),
    ("SF", "SEA", 300),
    ("SEA", "CHI", 2000),
    ("SEA", "LND", 500),
    ("LND", "SEA", 500),
    ("LND", "CHI", 1000),
    ("CHI", "NDL", 180)
]

schema = StructType([
    StructField("from", StringType(), True),
    StructField("to", StringType(), True),
    StructField("dist", IntegerType(), True)
])

df1 = spark.createDataFrame(data, schema)

df1.show()


joineddf = (
    df1.alias("a").join(df1.alias("b"), (col("a.to") == col("b.from")) & (col("a.from") == col("b.to")))
    .where(col("a.from") < col("a.to"))
    .select(
        col("a.from").alias("from"),
        col("a.to").alias("to"),
        (col("a.dist") + col("b.dist")).alias("roundtrip_dist")
    )
)
joineddf.show()
