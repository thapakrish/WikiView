from __future__ import print_function
from pyspark import SparkContext, SparkConf
from pyspark.sql import Row
from pyspark.sql import SQLContext
import os
import pyspark_cassandra
import rethinkdb as r

conf = SparkConf().setAppName("MergeTables")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)



conn = r.connect(host='localhost', port=28015, db='test')
cursor = r.table("wiki_graph").run(conn)
#r.table('wiki_graph').insert({
#    "pgfrom": 1,
#    "pgto": 2,
#    "pgtoto" : [1,2],
#}).run(conn)
 
for document in cursor:
    print(document)

"""
r.table("wiki_graph1").index_create(
    "from_to", [r.row["pgfrom"], r.row["pgto"]]
).run(conn)
"""
