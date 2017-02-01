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

# Grab AWS credentials from the environment
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')

# Make a connection to S3 via boto
bucket_name = "kt-wiki"
folder_name = "/"
conn = boto.connect_s3(aws_access_key, aws_secret_access_key)
bucket = conn.get_bucket(bucket_name)

k = Key(bucket)
k.key = 'foobar.txt'
k.set_contents_from_string('This is a test of S3')
data = k.get_contents_as_string()
print data
