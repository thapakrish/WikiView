################################################################################
# Spark Entry Code
# Testing MapReduce Operations on raw data
#
#  Krishna Thapa                                                       Jan 2017
################################################################################

from pyspark import SparkContext, SparkConf
from pyspark.sql import Row
from pyspark.sql import SQLContext

# Connect to SparkContext
conf = SparkConf().setAppName("WikiView") \
                  .setMaster("spark://ip-XXX-XX-X-XXX")

sc = SparkContext(conf = conf)


# PAGES, read from HDFS directory
pages = sc.textFile("hdfs://ip-XXX-XX-X-XXX:9000/test/pagesTest.csv")


# Split lines by comma, give rownames to elements
dfpages = pages.map(lambda x: x.split(',')) \
          .map(lambda x: Row(page_id = x[0], page_nmspc = x[1], page_title = x[2]))


# Print sample output to screen
print ("----Printing----")
print pages.take(20)
print ("----Printed----")
