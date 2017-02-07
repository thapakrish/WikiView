################################################################################
# Read files from S3
# Grab metadata from filename
# Create json document that Spark can handle with only the relevant fields
# Krishna Thapa                                                        Feb 2017
################################################################################

from collections import defaultdict
from StringIO import StringIO
from boto.s3.key import Key
import os
import boto
import logging
import json
import gzip
import sys

# create logger

logger = logging.getLogger('WikiView')
hdlr = logging.FileHandler('s3_spark_json.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.DEBUG)


# Grab aws credentials from os environment
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')


# Set aws bucket and connect
bucket_name = "kt-wiki"
folder_name = "/"
conn = boto.connect_s3(aws_access_key, aws_secret_access_key)

bucket = conn.get_bucket(bucket_name)
key = bucket.get_key(folder_name)

## Sample 
k = Key(bucket)
k.set_contents_from_string('This is a test of S3')
data = k.get_contents_as_string()
logger.info("Sending test file to S3: {}".format(data))

def convert_to_json(basepath, sendto):
    """
    Given a path in S3, will grab .gz file from that path to convert to json

    basepath: grab gz files from dir
    sendto: which folder to send json file to
    """

    k = bucket.new_key(basepath)

    filenames = []
    year = month = day = hrs = ''

    for key in bucket.list():
        thisfile = key.name.encode('utf-8')
        if 'projectviews' not in thisfile and 'sql' not in thisfile and '.gz' in thisfile and thisfile.startswith(basepath):
            # S3 key name is of the format kt-wiki/pageviews/2016/2016-06/pageviews-20160601-000000.gz
            # Split by / to get last element
            filenames.append(thisfile)
            logger.info("Processing file: {}".format(thisfile))
            fname = thisfile.split('/')

            # Get content from filename and save to local
            # Split again to Grab year, month, day, hour value from filename
            key.get_contents_to_filename('/home/ubuntu/WikiView/data/' + fname[-1])
            fname1 = fname[-1]
            data_time = fname1[:-3].split('-')
            year, month, day, hrs =  data_time[1][:4], data_time[1][4:6], data_time[1][-2:], data_time[-1]

            docname =  'pageviews-' +  year + '-' + month + '-' + day + '-' + hrs + '.json'
            dictlist = []

            # save file from s3 to local, read, write to json, push json to s3
            with open(docname, 'w') as fp:
                #
                with gzip.open('/home/ubuntu/WikiView/data/'+fname[-1],'r') as fin:
                    for line in fin:
                        line = line.split(' ')
                        doc = {}
                        doc['ymdh'] = year + '-' + month + '-' + day + '-' + hrs
                        try:
                            # format: project, title, views, bytes ~ en Main_Page 242332 4737756101
                            prj, title, vcount = line[0], line[1], line[2]
                            doc['prj'] = prj
                            doc['title'] = title
                            doc['vcount'] = vcount
                            json.dump(doc,fp)
                            fp.write('\n')
                        except:
                            logger.error('Error reading gzip file {} at line: {}'.format(thisfile, line))
                            pass
#                            sys.exc_clear()

            # Now, save the json file to 
            key_name = 'pageviews-' +  year + '-' + month + '-' + day + '-' + hrs + '.json'
            full_key_name = os.path.join(sendto, key_name)
            k = bucket.new_key(full_key_name)

            logger.info("Sending json file to S3: {}".format(docname))
            k.set_contents_from_filename(key_name)

            # Remove temp file
            logger.info("Removing temp file: {} {}".format('/home/ubuntu/WikiView/data/', fname[-1]))
            os.remove('/home/ubuntu/WikiView/data/'+fname[-1])
            logger.info("Removing temp file: {}".format(key_name))
            os.remove(key_name)
    logger.info('Finished!!!')


if __name__ == "__main__":
    logging.basicConfig(filename='s3_spark_json.log', level=logging.INFO)
    logging.info('Starting...')

    basepath = 'pageviews/2016/2016-12/pageviews-201612'
    sendto = 'processed/'
    convert_to_json(basepath, sendto)
    logging.info('Finished...')
