import os
import sys


python_path = sys.executable
os.environ['PYSPARK_PYTHON']= python_path
os.environ['JAVA_HOME']=r'C:\Users\veera\.jdks\ms-17.0.15'

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("UserPageSequence").getOrCreate()

df = spark.read.option("header","true").option('mode','DROPMALFORMED').csv("file:///C:/Users/veera/Downloads/test.csv")
df.show()

