import os
import sys

from pyspark.sql.functions import concat

python_path = sys.executable
os.environ['PYSPARK_PYTHON']= python_path
os.environ['JAVA_HOME']=r'C:\Users\veera\.jdks\ms-17.0.15'

from pyspark.sql import SparkSession

from pyspark.sql.functions import *
spark = SparkSession.builder.appName("MultiColumnExample").getOrCreate()

data = [("m1", "m1,m2", "m1,m2,m3", "m1,m2,m3,m4")]
columns = ["col1", "col2", "col3", "col4"]

df = spark.createDataFrame(data, columns)
df.show(truncate=False)

mergedf = df.withColumn("col",expr("concat(col1,'~',col2,'~',col3,'~',col4)")).drop("col1","col2","col3","col4")
mergedf.show(truncate=False)

flattendf = mergedf.selectExpr("explode(split(col,'~'))as col").show()