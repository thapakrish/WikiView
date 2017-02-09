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
import logging

# create logger

logger = logging.getLogger('WikiView')
hdlr = logging.FileHandler('write_to_cassandra.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.DEBUG)


conf = SparkConf().setAppName("WikiView")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

#/usr/local/spark/bin/spark-submit --master spark://ip-172-31-1-103:7077 --executor-memory 2500M --driver-memory 5g s3json_mapreduce.py

pageviews = sqlContext.read.json("s3a://kt-wiki/test/*.json")


print ("pageviews data type: ", type(pageviews) )
#print pageviews.take(5)


#pageviews.registerTempTable("pageviews")
#pageviews.printSchema()

#print ("pageviews data type: ", type(pageviews) )

#largeviews = sqlContext.sql("SELECT title, ymdh, vcount FROM pageviews WHERE  vcount >= 10")

#largeviews = sqlContext.sql("SELECT title, ymdh, vcount FROM pageviews where vcount >= 1")

def print_result(res):
    print("############################################")
    print(res)
    print("############################################")

#lv = largeviews.take(200)       

#print_result(lv)
#2016-12-31-000000
rdd = pageviews.map(lambda x: (x['title'], x['ymdh'], x['vcount']))


# ymdh[:10] ==> day
# ymdh{:13] ==> hour
result = rdd.take(200)
print_result(result)
print (pageviews.take(20))
"""
# Create table first

CREATE KEYSPACE wiki_test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor': 3};

CREATE TABLE wiki_test.test1 (title varchar, ymdh varchar, vcount int, PRIMARY KEY (title, ymdh) );

CREATE TABLE wiki_test.test1 (title varchar, ymdh varchar, vcount int, PRIMARY KEY (title) );
"""

logger.info("Begin writing to Cassandra")
rdd.saveToCassandra("wiki_test","test1")
logger.info("Done writing to Cassandra")

# example query: select * from wiki_test.test1 limit 10;
