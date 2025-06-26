import os
import sys

python_path = sys.executable
os.environ['PYSPARK_PYTHON']= python_path
os.environ['JAVA_HOME']=r'C:\Users\veera\.jdks\ms-17.0.15'

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Senario-5").getOrCreate()

df1 = (spark.read
       .option("header",True)
       .option("inferSchema",True)
       .csv("file:///C:/Users/veera/Downloads/df1.csv"))
df1.show()
print(df1.rdd.getNumPartitions())

df2 = (spark.read
       .option("header","true")
       .option("inferSchema","true")
       .csv("file:///C:/Users/veera/Downloads/df2.csv"))
df2.show()
df2.persist()

df3 = df1.withColumn("salary",expr("CAST(1000 as INT)")) # we can also use lit(1000) which is use to add constant value in it
df3.show()
df3.persist()

df4 = df2.union(df3).orderBy("id")
df4.show()

filterdf4 = df4.filter(col("email").rlike("@")) # we can also use contains("@")
filterdf4.show()

writeresult = (filterdf4.write.format("csv").partitionBy("salary").save("file:///C:/Users/veera/Downloads/writeDataParquet"))

print("data written")