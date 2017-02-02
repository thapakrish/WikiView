################################################################################
# Read json files from S3
# MapReduce operations on json objects from each json file
# 
# Krishna Thapa                                                        Feb 2017
################################################################################


from pyspark import SparkContext, SparkConf
from pyspark.sql import Row
from pyspark.sql import SQLContext
import os
from boto.s3.connection import S3Connection
import json

conf = SparkConf().setAppName("WikiView").setMaster("spark://ip-XXX-XX-X-XXX")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

#/usr/local/spark/bin/spark-submit --master spark://ip-172-31-1-103:7077 --executor-memory 2500M --driver-memory 5g s3json_mapreduce.py

pageviews = sqlContext.read.json("s3a://kt-wiki/test/*.json")

print pageviews.take(5)


pageviews.registerTempTable("pageviews")
pageviews.printSchema()

largeviews = sqlContext.sql("SELECT title, ymdh, vcount FROM pageviews WHERE  vcount >= 100")

print largeviews.take(20)

