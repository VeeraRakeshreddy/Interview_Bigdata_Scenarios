import os
import sys

python_path = sys.executable
os.environ['PYSPARK_PYTHON']= python_path
os.environ['JAVA_HOME']=r'C:\Users\veera\.jdks\ms-17.0.15'

from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("SalesData").getOrCreate()

data = [
    (1, 100, 2010, 25, 5000),
    (2, 100, 2011, 16, 5000),
    (3, 100, 2012, 8,  5000),
    (4, 200, 2010, 10, 9000),
    (5, 200, 2011, 15, 9000),
    (6, 200, 2012, 20, 7000),
    (7, 300, 2010, 20, 7000),
    (8, 300, 2011, 18, 7000),
    (9, 300, 2012, 20, 7000)
]

schema = StructType([
    StructField("sale_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("year", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", IntegerType(), True)
])

df_sales = spark.createDataFrame(data, schema)

df_sales.show()

win = Window.partitionBy("year").orderBy(col("quantity").desc())

rankdf = (df_sales.withColumn("rank",dense_rank().over(win))
          .filter(col("rank")==1).drop("rank")
          .orderBy(col("sale_id"))
          )
rankdf.show()