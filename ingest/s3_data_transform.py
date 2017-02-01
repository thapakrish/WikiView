################################################################################
# Read files from S3
# Grab metadata from filename
# Create json document with only the relevant fields
# Krishna Thapa                                                        Jan 2017
################################################################################

from pyspark import SparkContext, SparkConf
from pyspark.sql import Row
from pyspark.sql import SQLContext
import os
import boto
from boto.s3.key import Key
import logging
from collections import defaultdict
import json

# Grab AWS credentials from the environment
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')

# Make a connection to S3 via boto
bucket_name = "kt-wiki"
folder_name = "/"
conn = boto.connect_s3(aws_access_key, aws_secret_access_key)
bucket = conn.get_bucket(bucket_name)

"""
# Create foobar.txt file with some text on S3
k = Key(bucket)
k.key = 'foobar.txt'
k.set_contents_from_string('This is a test of S3')
data = k.get_contents_as_string()
print data
"""

# Iterate all files under the main bucket
# Create a list of dicts from each line and save to json with fieldnames
filenames = []
pagecounts = defaultdict(int)


path = '/'
k = bucket.new_key(path)

for key in bucket.list():
    thisfile = key.name.encode('utf-8')
    # test for one particular file
    if 'projectviews' not in thisfile and 'sql' not in thisfile and '.gz' in thisfile and thisfile.startswith("pageviews/2016/2016-12/pageviews-20161231-23"):
        filenames.append(thisfile)
        print thisfile
    if thisfile == 'pageviews/2016/2016-12/pageviews-20161231-230000.gz':
        # grab metadata from filename, make a json doc
        print ("Found the field")
        fname = thisfile.split('/')
        # save from from S3 to local
        key.get_contents_to_filename('/home/ubuntu/spark/data/' + fname[-1])
        fname1 = fname[-1]
        data_time = fname1[:-3].split('-')
        year, month, day, hrs =  data_time[1][:4], data_time[1][4:6], data_time[1][-2:], data_time[-1]
        print ("formatted dt: ", data_time,  year, month, day, hrs)

        doc = {}
        doc['ymdh'] = year + '-' + month + '-' + day + '-' + hrs
        count = 0

        # Read file, save processed output to json
        with open('data.json', 'w') as fp:
            with gzip.open('/home/ubuntu/spark/data/'+fname[-1],'r') as fin:
                for line in fin:
                    line = line.split(' ')
                    try:
                        proj, title, vcount = line[0], line[1], line[2]
                        doc['title'] = title
                        doc['vcount'] = vcount
                    except:
                        print "Error due to bad format!"
                        print line
                    pagecounts[proj] += 1
                    count += 1
                    if count <= 15:
                        print(year, month, day, hrs, title.decode('utf-8'), vcount, proj)
                        dictlist.append(doc)
                    else:
                        break
            json.dump(dictlist, fp)

# Upload the file to S3
key_name = 'pageviews-' +  year + '-' + month + '-' + day + 'data.json'
path = '/test/'
full_key_name = os.path.join(path, key_name)
k = bucket.new_key(full_key_name)
k.set_contents_from_filename('data.json')




#print data
#print pagecounts
print sorted(pagecounts.iteritems(),key=lambda (k,v): v,reverse=True)
print count
