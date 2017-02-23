################################################################################
# Write tables to Cassandra
# 
# Krishna Thapa                                                        Feb 2017
################################################################################


from pyspark import SparkContext, SparkConf
from pyspark.sql import Row
from pyspark.sql import SQLContext
import os
import json
import pyspark_cassandra
import logging

# Creat Spark Context
conf = SparkConf().setAppName("LinksJoin")
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


def push_graph_to_cassandra(pageviews, keyspace, table):
    """
    Push graph from RDDs to Cassandra
    Key: page_title
    """

    rdd =  pageviews.filter(lambda x: x['vcount'] != None and x['prj'] == "en")\
                    .map(keyval_from_dict)\
                    .reduceByKey(lambda x, y : x + y)\
                    .map(tup_from_keyval)

    logger = logging.getLogger('WikiLog')
    logger.info("Begin writing {} to Cassandra".format('hourly table'))
    rdd.saveToCassandra(keyspace, table)
    logger.info("Done writing {} to Cassandra".format('hourly table'))

def is_ascii(s):
    return all(ord(c) < 128 for c in s)


def combine_page_pagelinks(dfpg, dfpl):
    dfpg.registerTempTable("dfpg")
    dfpl.registerTempTable("dfpl")
    
    # Combine two tables on common column, which is page_id, pl_from
    combined = sqlContext.sql('SELECT page_title, pl_title FROM dfpg JOIN dfpl ON dfpg.page_id = dfpl.pl_from')
    return combined

def combine_2nd_degree(combined):
    # For each pageTITLE, Get list of Hyperlinks
    # Step 1: get (page_title, pl_title)
    prepare = combined.map(lambda x: Row( page_title = x['page_title'], pl_title = x['pl_title']))

    prepare = prepare.toDF()

    combined = combined.map(lambda x: (x['page_title'], [x['pl_title']])) \
                       .reduceByKey(lambda x,y: x + y)\
                       .map(lambda x: Row(page = x[0], links = x[1]))
#                                          links = list(set(x[1]))))
    combined = combined.toDF()

    # Step 2: For each (Page, PageTO), add links to get 2nd degree connection for Page
    prepare.registerTempTable('prepare')
    combined.registerTempTable('combined')

    prepare.printSchema()
    combined.printSchema()

    final = sqlContext.sql('SELECT page_title, pl_title, links FROM prepare JOIN combined ON prepare.pl_title = combined.page')
    return final

if __name__ == "__main__":

    logger = configure_logger()

    # Load Data
    page_path = "hdfs://ip-XXX-XX-X-X:9000/test/page-2016-09-01.json"
    pages = sqlContext.read.json(page_path)

    
    links_path = "hdfs://ip-XXX-XX-X-X:9000/test/linksTest.json"
    pagelinks = sqlContext.read.json(links_path)

    print( "#######TYPES##########", type(pages), type(pagelinks))

    pages.printSchema()
    pagelinks.printSchema()

    # Combine pageID with pageTITLE
    combined = combine_page_pagelinks(pages, pagelinks)

    # Get 2nd degree connections
    final = combine_2nd_degree(combined)
    final = final.rdd


    final = final.map(lambda x: (x['page_title'],x['pl_title'], x['links']))



    # Send graph data to cassandra table

    final.saveToCassandra("graph", "g2")


    sc.stop()
