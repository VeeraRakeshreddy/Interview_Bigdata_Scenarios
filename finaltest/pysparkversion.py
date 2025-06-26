import os
import sys

from pyspark.sql.functions import split, explode

python_path = sys.executable
os.environ['PYSPARK_PYTHON']= python_path
os.environ['JAVA_HOME']=r'C:\Users\veera\.jdks\ms-17.0.15'

import pyspark
from pyspark.sql import SparkSession

print("PySpark Version:", pyspark.__version__)

spark = SparkSession.builder.getOrCreate()
print(spark.version)