################################################################################
# Write tables to Cassandra
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

# Creat Spark Context
conf = SparkConf().setAppName("WikiView")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)


# Load Data
pageviews = sqlContext.read.json("s3a://kt-wiki/processed/pageviews-2016-12-02*.json")
pageviews = pageviews.rdd


# pageviews.registerTempTable("pageviews")
# pageviews.printSchema()

def configure_logger():
    logger = logging.getLogger('WikiLog')
    hdlr = logging.FileHandler('write_to_cassandra.log')
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr) 
    logger.setLevel(logging.INFO)
    return logger


def keyval_from_dict(rdd):
    """
    Make a key value pair for partition from a dict object
    Convert string vcount value to int
    """
    # >>> x = '2016-12-31-000000'
    # >>> a[:13]
    # '2016-12-31-00'

    a = rdd['title']
    b = rdd['ymdh'][:13]
    c = rdd['vcount']
    return ((a,b),int(c))


def tup_from_keyval(tup):
    """
    Flatten tuple object ==> ((a,b),c) to (a,b,c)
    """

    a = tup[0][0]
    b = tup[0][1]
    c = tup[1]
    return (a,b,c)


def push_hourly_to_cassandra(keyspace, table):
    """
    Push hourly data from RDDs to Cassandra
    Key: title, year-month-day-hour
    """

    rdd =  pageviews.filter(lambda x: x['vcount'] != None)\
                    .map(keyval_from_dict)\
                    .reduceByKey(lambda x, y : x + y)\
                    .map(tup_from_keyval)

    logger = logging.getLogger('WikiLog')
    logger.info("Begin writing {} to Cassandra".format('hourly table'))
    rdd.saveToCassandra(keyspace, table)
    logger.info("Done writing {} to Cassandra".format('hourly table'))


def push_daily_to_cassandra(keyspace, table):
    """
    Push daily data from RDDs to Cassandra
    First, sum by key where key ==> (title, year-month-day)
    """

    rdd = pageviews.filter(lambda x: x['vcount'] != None)\
                   .map(lambda x: (x['title'], x['ymdh'], x['vcount']))\
                   .map(lambda x: ((x[0],x[1]), x[2]))\
                   .reduceByKey(lambda x, y: x + y)

    logger.info("Begin writing {} to Cassandra".format('daily table'))
    rdd.saveToCassandra(keyspace, table)
    logger.info("Done writing {} to Cassandra".format('daily table'))

def push_max_val_to_cassandra(keyspace, table):
    """
    For each title, get the maximum viewcount for given date range
    """

    rdd = pageviews.filter(clean_rdd)\
                   .map(lambda x: (x['title'], x['ymdh'], x['vcount']))\
                   .map(lambda x: ((x[0],x[1]), x[2]))\
                   .reduceByKey(max)

    logger.info("Begin writing {} to Cassandra".format('max table'))
    rdd.saveToCassandra(keyspace, table)
    logger.info("Done writing {} to Cassandra".format('max table'))


    

if __name__ == "__main__":

    """    
    CREATE KEYSPACE wiki WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor': 3};
    CREATE TABLE wiki.hourly (title varchar, ymdh varchar, vcount int, PRIMARY KEY (title, ymdh) );
    """

    logger = configure_logger()
    push_hourly_to_cassandra("wiki", "hourly")


    """    
    CREATE KEYSPACE wiki WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor': 3};
    CREATE TABLE wiki.daily (title varchar, ymdh varchar, vcount int, PRIMARY KEY (title, ymdh) );
    """
#    push_daily_to_cassandra(logger, "wiki", "daily")


    """    
    CREATE KEYSPACE wiki WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor': 3};
    CREATE TABLE wiki.hourly (title varchar, ymdh varchar, vcount int, PRIMARY KEY (title, ymdh) );
    """
#    push_max_to_cassandra(logger, "wiki", "max")
