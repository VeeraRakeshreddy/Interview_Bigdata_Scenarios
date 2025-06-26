import os
import sys


python_path = sys.executable
os.environ['PYSPARK_PYTHON']= python_path
os.environ['JAVA_HOME']=r'C:\Users\veera\.jdks\ms-17.0.15'

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("EmployeeDepartments").getOrCreate()

emp_data = [
    (1, "A", "A", 1000000),
    (2, "B", "A", 2500000),
    (3, "C", "G", 500000),
    (4, "D", "G", 800000),
    (5, "E", "W", 9000000),
    (6, "F", "W", 2000000)
]
emp_columns = ["emp_id", "name", "dept_id", "salary"]

df_emp = spark.createDataFrame(emp_data, emp_columns)
df_emp.show()

dept_data = [
    ("A", "AZURE"),
    ("G", "GCP"),
    ("W", "AWS")
]
dept_columns = ["dept_id1", "dept_name"]

df_dept = spark.createDataFrame(dept_data, dept_columns)
df_dept.show()

joineddf = df_emp.join(df_dept,df_emp["dept_id"]==df_dept["dept_id1"],"inner").drop("dept_id","dept_id1")
joineddf.show()

windowdf = Window.partitionBy("dept_name").orderBy("salary")

rankeddf = joineddf.withColumn("rank",rank().over(windowdf)).filter(col("rank")==1).drop("rank")
rankeddf.show()

#sql

df_emp.createOrReplaceTempView("emp")
df_dept.createOrReplaceTempView("dept")

result = spark.sql("SELECT emp_id,name,dept_name,salary FROM (SELECT a.emp_id,a.name,b.dept_name,a.salary,rank() over ( partition by dept_name order by salary desc) as rank FROM emp a join dept b on a.dept_id = b.dept_id1)as source where rank = 2")

result.show()
