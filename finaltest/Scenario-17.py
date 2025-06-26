import os
import sys


python_path = sys.executable
os.environ['PYSPARK_PYTHON']= python_path
os.environ['JAVA_HOME']=r'C:\Users\veera\.jdks\ms-17.0.15'

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MergeEmployees").getOrCreate()

data1 = [
    (1, "Tim", 24, "Kerala", "India"),
    (2, "Asman", 26, "Kerala", "India")
]
schema1 = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True)
])
df1 = spark.createDataFrame(data1, schema1)

data2 = [
    (1, "Tim", 24, "Comcity"),
    (2, "Asman", 26, "bimcity")
]
schema2 = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("address", StringType(), True)
])
df2 = spark.createDataFrame(data2, schema2)

df1.show()
df2.show()

#one way
mergedf = df1.join(df2,["emp_id","name","age"],'inner').show()

#another ways by renaming the columns
#joindf = df1.join(df2,df1['emp_id'] == df2['emp_id2'],'inner').drop("emp_id2","name2","age2").show()