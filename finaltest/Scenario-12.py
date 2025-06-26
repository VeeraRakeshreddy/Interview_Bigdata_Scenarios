import os
import sys



python_path = sys.executable
os.environ['PYSPARK_PYTHON']= python_path
os.environ['JAVA_HOME']=r'C:\Users\veera\.jdks\ms-17.0.15'

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

def mask_mail(email):
    return (email[0]+'*****************'+email[-10:])



def mask_mobile(mobile):

     return (mobile[0]+'******'+mobile[-3:])

data = [
    ("Renuka1992@gmail.com", "9856765434"),
    ("anbu.arasu@gmail.com", "9844567788")
]
columns = ["email", "mobile"]

df = spark.createDataFrame(data, columns)
df.show()

#with UDF
maskeddf = (df.withColumn("email",udf(mask_mail)(df.email))
            .withColumn("mobile",udf(mask_mobile)('mobile'))
           # .show(truncate=False)
            )
#without UDF
anotherway = (df.withColumn("email",concat(substring('email',0,2),lit("********************"),substring('email',-10,10)))
              .withColumn("mobile",concat(substring('mobile',0,2),lit("******"),substring('mobile',-2,2)))
              .show(truncate=False))