import os
import sys

python_path = sys.executable
os.environ['PYSPARK_PYTHON']= python_path
os.environ['JAVA_HOME']=r'C:\Users\veera\.jdks\ms-17.0.15'

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()
data = [
    ("India",),
    ("Pakistan",),
    ("SriLanka",)
]
myschema = ["teams"]
df = spark.createDataFrame(data, schema=myschema)
df.show()

joineddf = df.alias("a").join(df.alias("b"),col("a.teams") < col("b.teams"), "inner")
#joineddf.show()

concatdf = joineddf.withColumn("Match",expr("concat(a.teams,' Vs ',b.teams)")).drop("teams","teams")
concatdf.show()