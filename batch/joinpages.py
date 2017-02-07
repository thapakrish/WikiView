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

# PAGES
#pages = sc.textFile("hdfs://ip-172-31-1-106:9000/test/pageTest.csv")
pages = sc.textFile("hdfs://ip-172-31-1-106:9000/test/page-2016-09-01.csv")
dfpages = pages.map(lambda x: x.split(',')) \
          .map(lambda x: Row(page_id = x[0], page_nmspc = x[1], page_title = x[2]))

#df1 = dfpages.toDF(['page_id', 'page_nmsc', 'page_title'])
dfpg = sqlContext.createDataFrame(dfpages)


# PAGELINKS

#pagelinks = sc.textFile("hdfs://ip-172-31-1-106:9000/test/pagelinksTest.csv")
pagelinks = sc.textFile("hdfs://ip-172-31-1-106:9000/test/pagelinks-20160901.csv")
dfplinks = pagelinks.map(lambda x: x.split(',')) \
              .map(lambda x: Row(pl_from = x[0], pl_from_nmspc = x[1], pl_nmspc = x[2], pl_title = x[3]))

#dfpl = d2.toDF(['pl_from', 'pl_from_nmspc', 'pl_nmspc', 'pl_title'])

dfpl = sqlContext.createDataFrame(dfplinks)


#dftest = df1.join(dfpl, dfpl.pl_title == df1.page_title)
#dftest = sqlContext.sql("SELECT pl_from AS pl_from, pl_tille as pl_title from d2")

"""
print ("----Printing----")
print pages.take(20)
print ("----Printed----", type(dfpg), type(dfpl))
"""

print ("----Printed----", type(dfpg), type(dfpl))

dfpg.printSchema()
dfpl.printSchema()

#df.select(df['name'], df['age'] + 1).show()

#JOIN
#>>> cond = [df.name == df3.name, df.age == df3.age]
#>>> df.join(df3, cond, 'outer').select(df.name, df3.age).collect()
"""
cond = [dfpg.page_title == dfpl.pl_title, dfpg.page_nmspc == dfpl.pl_nmspc]

temp1 = dfpg.join(dfpl, cond, 'outer').select(dfpg.page_title, dfpg.page_nmspc)
temp2 = temp1.groupBy("page_title")

temp3 = dfpl.groupBy("pl_from")


print(type(temp1), type(temp2), type(temp3))
temp4 = temp3.count().collect()

#print (temp1.take(20))
#print (temp2.count().collect())

print(type(temp4))
newlist = sorted(temp4, key=lambda x: x.pl_from, reverse=True)

#print(newlist)
"""


dfpg.registerTempTable("pages")
dfpl.registerTempTable("pagelinks")


sqlContext.sql(
    "select page_id, page_nmspc, page_title from pages").show(30)

sqlContext.sql(
    "select pl_from, pl_from_nmspc, pl_nmspc, pl_title from pagelinks").show(30)

#sqlContext.sql(
#  "select pl_from, pl_from_nmspc, pl_nmspc, pl_title from pagelinks").show()


# sqlContext.sql('SELECT * FROM pages JOIN pagelinks ON pages.page_title = pagelinks.pl_title').show()
#sqlContext.sql('SELECT * FROM pages JOIN pagelinks ON pages.page_title = pagelinks.pl_title and  pages.page_nmspc = pagelinks.pl_nmspc').show()


# dfpl.groupBy("pl_from").agg(GroupConcat("pl_nmspc")).show()
# people.filter(people.age > 30).join(department, people.deptId == department.id))           .groupBy(department.name, "gender").agg({"salary": "avg", "age": "max"})


#>>> rdd = sc.parallelize([1, 2, 3, 4, 5])
#>>> rdd.filter(lambda x: x % 2 == 0).collect()

pgrdd = dfpg.rdd
plrdd = dfpl.rdd

pgrdd = pgrdd.map(lambda x: (x['page_id'], x['page_title']))
plrdd = plrdd.filter(lambda x: x['pl_title'] !='0')\
             .map(lambda x: (x['pl_from'], [x['pl_title']]))\
             .reduceByKey(lambda a, b: a + b)
#             .reduceByKey(add).collect()

print ("----Printed----", type(plrdd), type(pgrdd))

#plrdd1 = plrdd.map(lambda x: x.page_id)
# print (plrdd1.take(10))
print (pgrdd.take(20))
print (sorted(plrdd.take(20)))


sc.stop()
