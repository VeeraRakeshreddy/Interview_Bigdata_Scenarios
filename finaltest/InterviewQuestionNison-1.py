import os
import sys


python_path = sys.executable
os.environ['PYSPARK_PYTHON']= python_path
os.environ['JAVA_HOME']=r'C:\Users\veera\.jdks\ms-17.0.15'

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SimpleDataFrames").getOrCreate()

df1 = spark.createDataFrame([(1,), (2,), (1,)], ["id"])

df2 = spark.createDataFrame([(1,), (2,), (1,)], ["id"])

#df1.show()
#df2.show()

innerjoin = df1.join(df2,df1['id']==df2['id'],'inner').show()
leftjoin = df1.join(df2,df1['id']==df2['id'],'left').show()
rightjoin = df1.join(df2,df1['id']==df2['id'],'right').show()
outerjoin = df1.join(df2,df1['id']==df2['id'],'outer').show()
crossjoin = df1.crossJoin(df2)
crossjoin.show()

crossjoin.write.save("s3://rakeshreddys3/test")



