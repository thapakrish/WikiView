#!/bin/bash
# sample script to bulk copy files from master node onto HDFS
for FILE in `ls ~/dumps.wikimedia.org/other/pageviews/2015/2015-05/ | sort -gr | head -1`
do hdfs dfs -copyFromLocal -f ~/dumps.wikimedia.org/other/pageviews/2015/2015-05/$FILE /pageviews/2010
5/2015-05/
done
