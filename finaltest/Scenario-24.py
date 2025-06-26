import os
import sys

from pyspark.sql.functions import *

python_path = sys.executable
os.environ['PYSPARK_PYTHON']= python_path
os.environ['JAVA_HOME']=r'C:\Users\veera\.jdks\ms-17.0.15'

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("UserPageSequence").getOrCreate()

data = [
    (1, "home"), (1, "products"), (1, "checkout"), (1, "confirmation"),
    (2, "home"), (2, "products"), (2, "cart"), (2, "checkout"),
    (2, "confirmation"), (2, "home"), (2, "products")
]

schema = StructType([
    StructField("userid", IntegerType(), True),
    StructField("page", StringType(), True)
])

df = spark.createDataFrame(data, schema)
df.show()

resultdf = df.groupby("userid").agg(collect_list(col("page")).alias("page"))
resultdf.show(truncate=False)