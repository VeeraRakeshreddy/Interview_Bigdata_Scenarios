import os
import sys



python_path = sys.executable
os.environ['PYSPARK_PYTHON']= python_path
os.environ['JAVA_HOME']=r'C:\Users\veera\.jdks\ms-17.0.15'

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("FoodRatings").getOrCreate()

food_data = [
    (1, "Veg Biryani"),
    (2, "Veg Fried Rice"),
    (3, "Kaju Fried Rice"),
    (4, "Chicken Biryani"),
    (5, "Chicken Dum Biryani"),
    (6, "Prawns Biryani"),
    (7, "Fish Birayani")
]
food_columns = ["food_id", "food_item"]

df_food = spark.createDataFrame(food_data, food_columns)
df_food.show()

rating_data = [
    (1, 5),
    (2, 3),
    (3, 4),
    (4, 4),
    (5, 5),
    (6, 4),
    (7, 4)
]
rating_columns = ["food_id", "rating"]

df_rating = spark.createDataFrame(rating_data, rating_columns)
df_rating.show()

joineddf = df_food.join(df_rating,['food_id'],"inner").select(df_rating['food_id'],"food_item","rating")
joineddf.show()

newcol = joineddf.withColumn("state(out of 5)",expr("repeat('*',rating)"))
newcol.show()