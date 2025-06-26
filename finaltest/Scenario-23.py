import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

python_path = sys.executable
os.environ['PYSPARK_PYTHON']= python_path
os.environ['JAVA_HOME']=r'C:\Users\veera\.jdks\ms-17.0.15'



spark = SparkSession.builder.getOrCreate()

data = [(1,5),(2,6),(3,5),(3,6),(1,6)]
df1 = spark.createDataFrame(data,['customer_id','product_key'])
df1.show()

data1 = [(5,),(6,)]
df2 = spark.createDataFrame(data1,['product_key'])
df2.show()

joineddf = df1.join(df2,['product_key'],'inner').drop("product_key").distinct().filter(col("customer_id")!=2).show()