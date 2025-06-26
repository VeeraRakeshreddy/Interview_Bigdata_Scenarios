import os
import sys

from pyspark.sql.functions import explode

python_path = sys.executable
os.environ['PYSPARK_PYTHON']= python_path
os.environ['JAVA_HOME']=r'C:\Users\veera\.jdks\ms-17.0.15'

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import *

# Start Spark session
spark = SparkSession.builder.appName("CommissionData").getOrCreate()

# Define the data
data = [
    (1, 300, "31-Jan-2021"),
    (1, 400, "28-Feb-2021"),
    (1, 200, "31-Mar-2021"),
    (2, 1000, "31-Oct-2021"),
    (2, 900, "31-Dec-2021")
]

schema = StructType([
    StructField("empid", IntegerType(), True),
    StructField("commissionamt", IntegerType(), True),
    StructField("monthlastdate", StringType(), True)
])

df = spark.createDataFrame(data, schema)

df.show()

datecon = df.withColumn("monthlastdate",to_date("monthlastdate","dd-MMM-yyyy"))
datecon.show()

recentdata = (datecon.groupby(col('empid').alias("empid1"))
              .agg(max(col('monthlastdate'))
                   .alias('monthlastdate1'))
              )
recentdata.show()

final_data = (datecon.join(recentdata,(datecon['empid'] == recentdata['empid1']) & (datecon['monthlastdate'] == recentdata['monthlastdate1']),"inner")
              .select("empid","commissionamt","monthlastdate")
              )

final_data.show()