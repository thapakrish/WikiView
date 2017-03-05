import csv
import json 



"""
    #    pages = sc.textFile("./pagesTest.csv")
    dfpages = pages.map(lambda x: x.split(',')) \
                   .filter(lambda x: int(x[1]) == 0) \
                   .map(lambda x: Row(page_id = int(x[0]), \
                                      page_title = x[2]))
"""

with open('page-2016-09-01.json', 'w') as fp:
    with open('/home/ubuntu/WikiView/page-2016-09-01.csv','r') as fin:
        for line in fin:
            row = line.split(',')
            if (int(row[1]) == 0):
                    doc = {"page_id": row[0], "page_title":row[2]}
                    json.dump(doc,fp)
                    fp.write('\n')



"""
    pagelinks = sc.textFile("hdfs://ip-172-31-1-107:9000/test/pagelinksTest.csv")
    #    pagelinks = sc.textFile("hdfs://ip-172-31-1-106:9000/test/pagelinks-20160901.csv")
    dfplinks = pagelinks.map(lambda x: x.split(',')) \
                        .map(lambda x: Row(pl_title = x[2], pl_from = int(x[0])))
    print(dfplinks.take(20))
    
"""

with open('pagelinks-2016-09-01.json', 'w') as fp:
#    with open('/home/ubuntu/WikiView/data/pagelinksTest.csv','r') as fin:
    with open('/home/ubuntu/WikiView/pagelinks-2016-09-01.csv','r') as fin:
        for line in fin:
            row = line.split(',')
            doc = {"pl_title": row[2], "pl_from":row[0]}
            json.dump(doc,fp)
            fp.write('\n')
