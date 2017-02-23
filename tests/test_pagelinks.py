################################################################################
#
# Join pages and pagelinks tables
#
#  Krishna Thapa                                                       Jan 2017
################################################################################

from pyspark import SparkContext, SparkConf
from pyspark.sql import Row
from pyspark.sql import SQLContext
import os
import pyspark_cassandra
import rethinkdb as r
import json

conf = SparkConf().setAppName("MergeTest1")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)


def clean_pages(pages):
    dfpages = pages.map(lambda x: x.split(',')) \
                   .filter(lambda x: int(x[1]) == 0) \
                   .map(lambda x: Row(page_id = int(x[0]), \
                                      page_title = x[2]))
    return dfpages

def clean_pagelinks(pagelinks):
    dfplinks = pagelinks.map(lambda x: x.split(',')) \
                        .map(lambda x: Row(pl_title = x[2], pl_from = int(x[0])))
    return dfplinks

def combine_page_pagelinks(dfpages, dfplinks):
    # Generate links for each pages using RDD
    dfpg = sqlContext.createDataFrame(dfpages)
    dfpl = sqlContext.createDataFrame(dfplinks)
    
    print ("----DataFrame Schemas----\n",dfpg.printSchema(), dfpl.printSchema())


    dfpg.registerTempTable("dfpg")
    dfpl.registerTempTable("dfpl")
    
    # Combine two tables on common column, which is page_id, pl_from
    combined = sqlContext.sql('SELECT * FROM dfpg JOIN dfpl ON dfpg.page_id = dfpl.pl_from')

    """
    print ("----taking few values a----", dfpg.take(20))
    print ("----taking few values b----", dfpl.take(20))
    print ("----taking few values c----", combined.take(20))
    """

    return combined

def is_ascii(s):
    return all(ord(c) < 128 for c in s)

if __name__ == "__main__":


    # Read pages file from HDFS
    pages = sc.textFile("hdfs://ip-172-31-1-107:9000/test/pagesTest.csv")
#    pages = sc.textFile("hdfs://ip-172-31-1-107:9000/test/page-2016-09-01.csv")
#    pages = sc.textFile("hdfs://ip-172-31-1-107:9000/test/pages2.csv")
    #    pages = sc.textFile("./pagesTest.csv")
    
    dfpages = clean_pages(pages)
    print(dfpages.take(20))

    # Working Solution
    """
    # Read pagelinks file from HDFS
    pagelinks = sc.textFile("hdfs://ip-172-31-1-107:9000/test/pagelinksTest.csv")
    #    pagelinks = sc.textFile("hdfs://ip-172-31-1-106:9000/test/pagelinks-20160901.csv")
    dfplinks = pagelinks.map(lambda x: x.split(',')) \
                        .map(lambda x: ((x[2], x[2]), [x[0]]))\
                        .reduceByKey(lambda x, y : x + y)\
                        .map(lambda x: ({"pgfrom": x[0][0], "pgto": x[0][1], "pgtoto": x[1]}))
    print(dfplinks.take(20))
    print("Type before saving: ", type(dfplinks))

    dfplinks.saveToCassandra("graph", "g1")
    """

    #    pagelinks = sc.textFile("hdfs://ip-172-31-1-107:9000/test/pagelinks-2016-09-01.csv")
    #    pagelinks = sc.textFile("hdfs://ip-172-31-1-107:9000/test/links2.csv")    
    pagelinks = sc.textFile("hdfs://ip-172-31-1-107:9000/test/pagelinksTest.csv")    
    dfplinks = clean_pagelinks(pagelinks)
    print(dfplinks.take(20))
    

    
    combined = combine_page_pagelinks(dfpages, dfplinks)
    #    plrdd = get_page_links(dfpg, dfpl)
    

    combined = combined.rdd.persist()
    print("RDD: ", combined.take(10))



    prepare = combined.filter(lambda x : is_ascii(x['page_title']) == True)\
                      .map(lambda x: Row( pl_title = x['pl_title'], page_title = x['page_title']))

    # Now Reduce by common key to get pages==>list of links
    combined = combined.map(lambda x: (x['pl_title'], [x['page_title']])) \
                       .reduceByKey(lambda x,y: x + y)\
                       .map(lambda x: Row(page = x[0], 
                                          links = list(set(x[1]))))
    print("Type of combined : ", type(combined))
    print(combined.take(5))

    # Now that we have page, pagelinks, we would like to get
    # page, links, links_from_links. To this end, we use prepare, which has 
    # original data with page_title to combine page, link, pagelinks
    # JOIN by pl_title


    prepare = prepare.toDF()
    combined = combined.toDF()

    prepare.registerTempTable('prepare')
    combined.registerTempTable('combined')

    prepare.printSchema()
    combined.printSchema()

#    print(" Final types: ", type(prepare), type(combined))
    final = sqlContext.sql('SELECT * FROM prepare JOIN combined ON prepare.pl_title = combined.page')

    final = final.rdd
    print( "Type of final: ", type(final))
    print(final.take(5))

    # Filter Relevant INFO, which is page_title, pl_from, links-links

    final = final.filter(lambda x: x['page_title'] != None)\
                 .map(lambda x: ((x['page_title'],x['pl_title']), x['links']))\
                 .reduceByKey(lambda x, y : x + y)\
                 .map(lambda x: ((x[0][0], x[0][1], list(set(x[1])))))
#                 .map(lambda x: Row(key = (x[0]), lst = list(set(x[1]))))
#                 .map(lambda x: {"pgfrom": x[0][0], "pgto":x[0][1], "pgtoto":x[1]})

    newRdd = final.toDF()

#    df = newRdd.rdd.persist()
#    print("##########Print before save:#########", type(newRdd), type(final), type(df))
#    print(df.take(5))
    print(final.take(5))

    final.saveToCassandra("graph", "g1")
    sc.stop()
    

