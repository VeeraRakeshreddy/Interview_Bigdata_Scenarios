import os
import sys


python_path = sys.executable
os.environ['PYSPARK_PYTHON']= python_path
os.environ['JAVA_HOME']=r'C:\Users\veera\.jdks\ms-17.0.15'

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Hierarchy").getOrCreate()

data = [
    ("A", "AA"),
    ("B", "BB"),
    ("C", "CC"),
    ("AA", "AAA"),
    ("BB", "BBB"),
    ("CC", "CCC")
]

df = spark.createDataFrame(data, ["child", "parent"])
df1 = spark.createDataFrame(data, ["child1", "parent1"])
df.show()


joineddf = df.join(df1,df["parent"]==df1["child1"],"left").drop("child1").dropna(subset=["parent1"]).withColumnRenamed("parent1","GrandParent")
joineddf.show()