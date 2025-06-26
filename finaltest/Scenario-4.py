import os
import sys

from pyspark.sql.functions import collect_list, collect_set

python_path = sys.executable
os.environ['PYSPARK_PYTHON']= python_path
os.environ['JAVA_HOME']=r'C:\Users\veera\.jdks\ms-17.0.15'

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("CustomerData").getOrCreate()

schema = StructType([
    StructField("custid", IntegerType(), True),
    StructField("custname", StringType(), True),
    StructField("address", StringType(), True)
])

data = [
    (1, "Mark Ray",     "AB"),
    (2, "Peter Smith",  "CD"),
    (1, "Mark Ray",     "EF"),
    (2, "Peter Smith",  "GH"),
    (2, "Peter Smith",  "CD"),
    (3, "Kate",         "IJ")
]

df = spark.createDataFrame(data, schema)

df.show()

result = (df.groupby("custid","custname").agg(
    collect_set(
        "address"
    ).alias("address")
)
          #.select("custid","custname","address")
          )

result.show()