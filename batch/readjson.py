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
import pyspark_cassandra


conf = SparkConf().setAppName("WikiView")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

#/usr/local/spark/bin/spark-submit --master spark://ip-172-31-1-103:7077 --executor-memory 2500M --driver-memory 5g s3json_mapreduce.py

pageviews = sqlContext.read.json("s3a://kt-wiki/test/*.json")

print pageviews.take(5)


pageviews.registerTempTable("pageviews")
pageviews.printSchema()


largeviews = sqlContext.sql("SELECT title, ymdh, vcount FROM pageviews WHERE  vcount >= 100")


def print_result(res):
    print("############################################")
    print(res)
    print("############################################")

lv = largeviews.take(200)       

print_result(lv)

rdd = largeviews.map(lambda x: (x['title'], x['ymdh'], x['vcount']))
result = rdd.take(200)

print_result(result)

"""
# Create table first

CREATE KEYSPACE wiki_test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor': 3};

CREATE TABLE wiki_test.test1 (title varchar, ymdh varchar, vcount int, PRIMARY KEY (title, ymdh) );
"""


rdd.saveToCassandra("wiki_test","test1")

# example query: select * from wiki_test.test1 limit 10;
