import os
import sys


python_path = sys.executable
os.environ['PYSPARK_PYTHON']= python_path
os.environ['JAVA_HOME']=r'C:\Users\veera\.jdks\ms-17.0.15'

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("TwoDataFrames").getOrCreate()

data1 = [(1,), (2,), (3,)]
df1 = spark.createDataFrame(data1, ["col"])
df1.show()

data2 = [(1,), (2,), (3,), (4,), (5,)]
df2 = spark.createDataFrame(data2, ["col1"])
df2.show()

df1maxvalue = df1.agg(max("col").alias("Max")).first()[0]
#df1maxvalue.show()

filterdf2 = df2.filter(~(col("col1").isin(df1maxvalue)))
filterdf2.show()