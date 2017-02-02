################################################################################
# Read files from S3
# Grab metadata from filename
# Create json document with only the relevant fields that Spark can handle
# Krishna Thapa                                                        Jan 2017
################################################################################



from collections import defaultdict
from StringIO import StringIO
from boto.s3.key import Key
import os
import boto
import logging
import json
import gzip


aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')



bucket_name = "kt-wiki"
folder_name = "/"
conn = boto.connect_s3(aws_access_key, aws_secret_access_key)

bucket = conn.get_bucket(bucket_name)
key = bucket.get_key(folder_name)


path = 'test/'

k = Key(bucket)
k.set_contents_from_string('This is a test of S3')
data = k.get_contents_as_string()


#key.set_contents_from_filename('data.json')
print data

filenames = []
pagecounts = defaultdict(int)

year = month = day = hrs = ''
path = 'pageviews/2016/2016-12/'
k = bucket.new_key(path)

for key in bucket.list():
    thisfile = key.name.encode('utf-8')
#    if 'projectviews' not in thisfile and 'sql' not in thisfile and '.gz' in thisfile and thisfile.startswit
h("pageviews/2016/2016-12/pageviews-20161231-2"):
    if 'projectviews' not in thisfile and 'sql' not in thisfile and '.gz' in thisfile and thisfile.startswith
("pageviews/2016/2016-12/pageviews-20161231-"):
        filenames.append(thisfile)
        print ("Processing file: ", thisfile)
        fname = thisfile.split('/')
        key.get_contents_to_filename('/home/ubuntu/spark/data/' + fname[-1])
        fname1 = fname[-1]
        data_time = fname1[:-3].split('-')
        year, month, day, hrs =  data_time[1][:4], data_time[1][4:6], data_time[1][-2:], data_time[-1]
        print ("formatted dt: ", data_time,  year, month, day, hrs)
        count = 0

        docname =  'pageviews-' +  year + '-' + month + '-' + day + '-' + hrs + '.json'

        dictlist = []
        with open(docname, 'w') as fp:
            with gzip.open('/home/ubuntu/spark/data/'+fname[-1],'r') as fin:
                for line in fin:
                    line = line.split(' ')
                    doc = {}
                    doc['ymdh'] = year + '-' + month + '-' + day + '-' + hrs
                    try:
                        title, vcount = line[1], line[2]
                        doc['title'] = title
                        doc['vcount'] = vcount
                    except:
                        print line
                        print "Error!"
                    count += 1
                    json.dump(doc,fp)
                    fp.write('\n')
                    if (count % 200000 == 0 ):
                        print (count, doc)

            key_name = 'pageviews-' +  year + '-' + month + '-' + day + '-' + hrs + '.json'
            path = '/test/'
            full_key_name = os.path.join(path, key_name)
            k = bucket.new_key(full_key_name)

            print ("Sending json file to S3: ", docname)
            k.set_contents_from_filename(key_name)

            # Remove temp file
            print ("Removing temp file: ", '/home/ubuntu/spark/data/'+fname[-1])
            os.remove('/home/ubuntu/spark/data/'+fname[-1])
