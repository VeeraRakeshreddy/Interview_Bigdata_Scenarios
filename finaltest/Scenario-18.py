import os
import sys

from pyspark.sql.functions import split

python_path = sys.executable
os.environ['PYSPARK_PYTHON']= python_path
os.environ['JAVA_HOME']=r'C:\Users\veera\.jdks\ms-17.0.15'

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("SingleWordDF").getOrCreate()

data = [("The Social Dilemma",)]

schema = StructType([
    StructField("word", StringType(), True)
])

df = spark.createDataFrame(data, schema)

df.show()

strreverse = df.withColumn("reversed",reverse('word')).show()

reverseddf = df.withColumn("reversed",array_join(expr("transform(split(word,' '),x-> reverse(x))")," ")).show()
