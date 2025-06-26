import os
import sys


python_path = sys.executable
os.environ['PYSPARK_PYTHON']= python_path
os.environ['JAVA_HOME']=r'C:\Users\veera\.jdks\ms-17.0.15'

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("RegionData").getOrCreate()

data = [
    ("East", "Jan", 100),
    ("East", "Feb", 120),
    ("West", "Jan", 80),
    ("West", "Feb", 90)
]

columns = ["region", "month", "amount"]

df1 = spark.createDataFrame(data, columns)

df1.show()


result = df1.groupBy("region").pivot("month").agg(first("amount"))
result.show()

#sql1
df_clean = df1.withColumn("month", trim(upper("month")))

df_clean.createOrReplaceTempView("sales")
result2 = spark.sql("SELECT region, first(CASE WHEN month = 'FEB' THEN amount END) as Feb, first(CASE WHEN month = 'JAN' THEN amount END)as Jan FROM sales GROUP BY region")
result2.show()

#sql2
pivot_result = spark.sql("""
    SELECT *
    FROM sales
    PIVOT (
        FIRST(amount) FOR month IN ('FEB', 'JAN')
    )
""")

pivot_result.show()


