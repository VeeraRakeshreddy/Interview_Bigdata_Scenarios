import os
import sys

from pyspark.sql.functions import split, explode

python_path = sys.executable
os.environ['PYSPARK_PYTHON']= python_path
os.environ['JAVA_HOME']=r'C:\Users\veera\.jdks\ms-17.0.15'

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("NestedSchema").getOrCreate()

# Nested struct for likeDislike
likeDislikeStruct = StructType([
    StructField("dislikes", LongType(), True),
    StructField("likes", LongType(), True),
    StructField("userAction", LongType(), True)
])

# Nested struct inside multiMedia array
multiMediaElementStruct = StructType([
    StructField("createAt", StringType(), True),
    StructField("description", StringType(), True),
    StructField("id", LongType(), True),
    StructField("likeCount", LongType(), True),
    StructField("mediatype", LongType(), True),
    StructField("name", StringType(), True),
    StructField("place", StringType(), True),
    StructField("url", StringType(), True)
])

# Root schema
schema = StructType([
    StructField("code", LongType(), True),
    StructField("commentCount", LongType(), True),
    StructField("createdAt", StringType(), True),
    StructField("description", StringType(), True),
    StructField("feedsComment", StringType(), True),
    StructField("id", LongType(), True),
    StructField("imagePaths", StringType(), True),
    StructField("images", StringType(), True),
    StructField("isdeleted", BooleanType(), True),
    StructField("lat", LongType(), True),
    StructField("likeDislike", likeDislikeStruct, True),
    StructField("lng", LongType(), True),
    StructField("location", StringType(), True),
    StructField("mediatype", LongType(), True),
    StructField("msg", StringType(), True),
    StructField("multiMedia", ArrayType(multiMediaElementStruct), True),
    StructField("name", StringType(), True),
    StructField("profilePicture", StringType(), True),
    StructField("title", StringType(), True),
    StructField("totalFeed", LongType(), True),
    StructField("userId", LongType(), True),
    StructField("videoUrl", StringType(), True)
])

# Create an empty DataFrame with this schema
df = spark.createDataFrame([], schema)
df.printSchema()

flattendf = (df.withColumn("multiMedia",explode('multimedia'))
             .selectExpr("code",
                         "commentCount",
                         "createdAt",
                         "description as m_description",
                         "feedsComment",
                         "id as m_id",
                         "imagePaths",
                         "images",
                         "isdeleted",
                         "lat",
                         "likeDislike.*",
                         "lng",
                         "location",
                         "mediatype as m_mediatype",
                         "msg",
                         "multiMedia.*",
                         "name as m_name",
                         "profilePicture",
                         "title",
                         "totalFeed",
                         "userId",
                         "videoUrl"

                         )
             )
flattendf.printSchema()
createcompdf = (flattendf.groupby('code','commentCount','createdAt','m_description','feedsComment','m_id','imagePaths','images','isdeleted','lat','dislikes', 'likes', 'userAction','lng','location','m_mediatype','msg','m_name','profilePicture','title','totalFeed','userId','videoUrl')
.agg(
    collect_list(
        struct(
            col("createAt"),
            col("description"),
            col("likeCount"),
            col("id"),
            col("mediatype"),
            col("name"),
            col("place"),
            col("url")
        )
    ).alias("multiMedia")
)
).select(struct(col('dislikes'),col('likes'),col('userAction')).alias("likeDislikes")).printSchema()
