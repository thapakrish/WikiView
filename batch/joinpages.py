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

conf = SparkConf().setAppName("MergeTables")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)


def test_sql(pages, pagelinks, n=30):
    """
    Testing Testing
    """

    sample_pages = sqlContext.sql(
        "select page_id, page_nmspc, page_title from pages").show(n)

    sample_pagelinks = sqlContext.sql(
        "select pl_from, pl_from_nmspc, pl_nmspc, pl_title from pagelinks").show(n)

    return sample_pages, sample_pagelinks



def join_pages_pagelinks(pages, pagelinks):
    """
    Join pages and pagelinks table using SQL methods
    """

    result = sqlContext.sql('SELECT * FROM pages JOIN pagelinks ON \
    pages.page_title = pagelinks.pl_title and  pages.page_nmspc = pagelinks.pl_nmspc')

    return result



def get_page_links(dfpg, dfpl):
    """
    Convert dataframe to RDDs, gather RDDs of same key to workers to get \
    a list of links to each keys = pages
    """

    pgrdd = dfpg.rdd
    plrdd = dfpl.rdd

    pgrdd = pgrdd.map(lambda x: (x['page_id'], x['page_title']))
    plrdd = plrdd.filter(lambda x: x['pl_title'] !='0')\
                 .map(lambda x: (x['pl_from'], [x['pl_title']]))\
                 .reduceByKey(lambda a, b: a + b)

    return plrdd


if __name__ == "__main__":

    # Read pages file from HDFS
    pages = sc.textFile("hdfs://ip-172-31-1-106:9000/test/page-2016-09-01.csv")
    dfpages = pages.map(lambda x: x.split(',')) \
                   .map(lambda x: Row(page_id = x[0], \
                                      page_nmspc = x[1], \
                                      page_title = x[2]))


    # Read pagelinks file from HDFS
    pagelinks = sc.textFile("hdfs://ip-172-31-1-106:9000/test/pagelinks-20160901.csv")
    dfplinks = pagelinks.map(lambda x: x.split(',')) \
                        .map(lambda x: Row(pl_from = x[0], \
                                           pl_from_nmspc = x[1], \
                                           pl_nmspc = x[2], \
                                           pl_title = x[3]))

    # Get sample pages
    sample_pages, sample_pagelinks = test_sql(pages, pagelinks, 20)
    print ("---Sample Pages-----\n",sample_pages)
    print ("---Sample PageLinks-----\n",sample_pagelinks)

    # Get pages and pagelinks combined into one table via common column
    combined = join_pages_pagelinks(pages, pagelinks)
    print ("---Combined Table-----\n", combined.show())


    # Generate links for each pages using RDD
    dfpg = sqlContext.createDataFrame(dfpages)
    dfpl = sqlContext.createDataFrame(dfplinks)

    print ("----DataFrame Schemas----\n",dfpg.printSchema(), dfpl.printSchema())


    dfpg.registerTempTable("pages")
    dfpl.registerTempTable("pagelinks")

    plrdd = get_page_links(dfpg, dfpl)

    print ("----Printed----", type(plrdd))
    print (pgrdd.take(20))

    sc.stop()
