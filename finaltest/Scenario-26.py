import os
import sys


python_path = sys.executable
os.environ['PYSPARK_PYTHON']= python_path
os.environ['JAVA_HOME']=r'C:\Users\veera\.jdks\ms-17.0.15'

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("TwoDataFrames").getOrCreate()

# DataFrame 1
data1 = [(1, "A"), (2, "B"), (3, "C"), (4, "D")]
schema1 = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])
df1 = spark.createDataFrame(data1, schema1)

# DataFrame 2
data2 = [(1, "A"), (2, "B"), (4, "X"), (5, "F")]
schema2 = StructType([
    StructField("id1", IntegerType(), True),
    StructField("name1", StringType(), True)
])
df2 = spark.createDataFrame(data2, schema2)

#df1.show()
#df2.show()

joineddf = df1.join(df2,df1['id']==df2['id1'],'outer')
#joineddf.show()

filterdf = joineddf.filter(
            (col("name")!=col("name1"))|
            col("name").isNull() |
             col("name1").isNull()
)
#filterdf.show()

withcolumndf = filterdf.withColumn("comments",expr("CASE WHEN name IS NULL THEN 'new in target' WHEN name1 IS NULL THEN 'new in source' ELSE 'Mismatch' end"))

withcolumndf.show()

idDf = withcolumndf.withColumn("id",coalesce(col("id"),col("id1"))).drop("id1","name","name1")
idDf.show()