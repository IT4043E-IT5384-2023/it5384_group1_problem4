import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit, cast
from pyspark.sql.types import StringType, MapType, BooleanType, IntegerType
import os
import json 

master_url = "spark://34.142.194.212:7077"
jar_home = "/opt/spark/jars/gcs-connector-latest-hadoop2.jar"

spark = SparkSession.builder.appName("Group6_Problem4")\
    .config("spark.jars", jar_home)\
    .config("spark.memory.offHeap.enabled", True)\
    .config("spark.memory.offHeap.size","16g")\
    .master(master_url).getOrCreate()

spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", "/opt/bucket_connector/lucky-wall-393304-3fbad5f3943c.json")
spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')

spark._jsc.hadoopConfiguration().set("fs.gs.outputstream.upload.buffer.size", "262144")
spark._jsc.hadoopConfiguration().set("fs.gs.outputstream.upload.chunk.size", "1048576")
spark._jsc.hadoopConfiguration().set("fs.gs.outputstream.upload.max.active.requests", "4")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")


def get_info(input, column):
    if (input != "" and input != "None"):
        input = eval(str(input))
    else:
        input = {"id":"None"}
    if column == "is_blue_verified":
        if "is_blue_verified" in input.keys():
            return input["is_blue_verified"] 
        else:
            return None
    else:
        if ("legacy" in input.keys() and column in input["legacy"].keys()) :
            return input["legacy"][column]
        else:
            return None

get_info_udf = udf(get_info, StringType())

def create_at(input):
    if input!= None:
        return input.split()[-1]
    else:
        return None

create_at_udf = udf(create_at, StringType())

wallet_info = spark.read.json("twitter_wallet/*")
twitter_info = spark.read.json("data_twitter.jsonl")

# Proprocessing twitter info
twitter_info = twitter_info.withColumn('twitter_username', col('twitter_username').cast(StringType()))\
                           .withColumn('verified', get_info_udf(col('twitter_profile'), lit('is_blue_verified')).cast(StringType()))\
                           .withColumn('create_at', get_info_udf(col('twitter_profile'), lit('created_at') ))\
                           .withColumn('create_at', create_at_udf(col('create_at')).cast(IntegerType()))\
                           .withColumn('default_profile', get_info_udf(col('twitter_profile'), lit('default_profile') ).cast(BooleanType()))\
                           .withColumn('default_profile_image', get_info_udf(col('twitter_profile'), lit('default_profile_image') ).cast(BooleanType()))\
                           .withColumn('description', get_info_udf(col('twitter_profile'), lit('description') ).cast(StringType()))\
                           .withColumn('favourites_count', get_info_udf(col('twitter_profile'), lit('favourites_count') ).cast(IntegerType()))\
                           .withColumn('friends_count', get_info_udf(col('twitter_profile'), lit('friends_count') ).cast(IntegerType()))\
                           .withColumn('followers_count', get_info_udf(col('twitter_profile'), lit('followers_count') ).cast(IntegerType()))\
                           .withColumn('statuses_count', get_info_udf(col('twitter_profile'), lit('statuses_count') ).cast(IntegerType()))\
                           .withColumn('location', get_info_udf(col('twitter_profile'), lit('location') ).cast(StringType()))\
                           .drop("twitter_profile")
wallet_info = wallet_info.withColumn('user_id', col('user_id').cast(StringType()))\
                         .withColumn('twitter_username', col('twitter_username').cast(StringType()))\
                         .withColumn('user_address', col('user_address').cast(StringType()))\

twitter_info = twitter_info.filter("twitter_username IS NOT NULL")
wallet_info = wallet_info.filter("twitter_username IS NOT NULL")


wallet_social = wallet_info.join(twitter_info,on = [wallet_info.twitter_username == twitter_info.twitter_username], how="left").filter("user_address IS NOT NULL")
wallet_social.drop('twitter_username').toPandas().to_json("wallet_twitter_preprocessed.jsonl", orient='records', force_ascii=False, lines=True)
spark.stop
