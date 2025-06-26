import os
import sys
from itertools import count

python_path = sys.executable
os.environ['PYSPARK_PYTHON']= python_path
os.environ['JAVA_HOME']=r'C:\Users\veera\.jdks\ms-17.0.15'

data = [1,2,3,4,5,6]

data.extend([7,9])

data.append([10,11])

print(data)
