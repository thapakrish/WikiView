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


def push_hourly_to_cassandra(pageviews, keyspace, table):
    """
    Push hourly data from RDDs to Cassandra
    Key: title, year-month-day-hour
    """

#    rdd =  pageviews.filter(lambda x: x['vcount'] != None)\
    rdd =  pageviews.filter(lambda x: x['vcount'] != None and x['prj'] == "en")\
                    .map(keyval_from_dict)\
                    .reduceByKey(lambda x, y : x + y)\
                    .map(tup_from_keyval)

    logger = logging.getLogger('WikiLog')
    logger.info("Begin writing {} to Cassandra".format('hourly table'))
    rdd.saveToCassandra(keyspace, table)
    logger.info("Done writing {} to Cassandra".format('hourly table'))


def push_daily_to_cassandra(pageviews, keyspace, table):
    """
    Push daily data from RDDs to Cassandra
    First, sum by key where key ==> (title, year-month-day)
    """

    rdd = pageviews.filter(lambda x: x['vcount'] != None and x['prj'] == "en")\
                   .map(lambda x: (x['title'], x['ymdh'][:10], int(x['vcount'])))\
                   .map(lambda x: ((x[0],x[1]), x[2]))\
                   .reduceByKey(lambda x, y: x + y)\
                   .map(lambda x: (x[0][1], x[1],x[0][0]))


    logger.info("Begin writing {} to Cassandra".format('daily table'))
    rdd.saveToCassandra(keyspace, table)
    logger.info("Done writing {} to Cassandra".format('daily table'))

    

if __name__ == "__main__":

    logger = configure_logger()
    days = range(25,29)
    days_padded = [format(x, '02') for x in days]

    print (" ------------- Days---------", days_padded)
    for days in days_padded:
        path = "s3a://kt-wiki/processed/pageviews-2016-12-" + days + "*.json"
        print (" ------------- pageviews ---------", path)
        # Load Data

        pageviews = sqlContext.read.json(path)

        pageviews = pageviews.rdd
        push_hourly_to_cassandra(pageviews, "wiki", "hourly")
        
        #push_daily_to_cassandra(pageviews, "wiki", "daily")
        #push_max_to_cassandra(pageviews, "wiki", "max")
    sc.stop()
