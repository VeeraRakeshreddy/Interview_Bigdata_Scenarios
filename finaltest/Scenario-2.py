import os
import sys

python_path = sys.executable
os.environ['PYSPARK_PYTHON']= python_path
os.environ['JAVA_HOME']=r'C:\Users\veera\.jdks\ms-17.0.15'

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import *

# Start Spark session
spark = SparkSession.builder.appName("OrderStatus").getOrCreate()

# Define schema
schema = StructType([
    StructField("orderid", IntegerType(), True),
    StructField("statusdate", StringType(), True),
    StructField("status", StringType(), True)
])

# Define the data
data = [
    (1, "1-Jan", "Ordered"),
    (1, "2-Jan", "dispatched"),
    (1, "3-Jan", "dispatched"),
    (1, "4-Jan", "Shipped"),
    (1, "5-Jan", "Shipped"),
    (1, "6-Jan", "Delivered"),
    (2, "1-Jan", "Ordered"),
    (2, "2-Jan", "dispatched"),
    (2, "3-Jan", "shipped"),
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show DataFrame
df.show(truncate=False)

# one way
filterdf = df.filter(
            (col("status") == 'dispatched') &
            (col("orderid").isin(
    * [row[0] for row in
       df.filter(col("status")=='Ordered')
       .select("orderid")
       .collect()
       ]
)
            )
)
#filterdf.show()

# second way
ordersdf = df.filter(col("status")=='Ordered').select("orderid").distinct()

dispaceddf = df.filter(col("status")== 'dispatched')

joindf = dispaceddf.join(ordersdf,"orderid",'inner')

joindf.show()