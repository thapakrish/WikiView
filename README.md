## Table of Contents

1. [Data Flow] (README.md#data-flow)
2. [Data] (README.md#data-flow)
3. [AWS Cluster] (README.md#aws-cluster)
4. [Batch Processing] (README.md#batch-processing)
5. [Cassandra] (README.md#cassandra)
6. [Flask] (README.md#flask)


## Data Flow

![](data/DataFlow.png)


## Data

[Back to Table of Contents] (README.md#table-of-contents)

A graph related problem requires two components. First is the graph itself, which has nodes and edges. To generate this graph, I downloaded the `pages.sql.gz` and `pagelinks.sql.gz` dump that wiki media provides [Sample](https://dumps.wikimedia.org/enwiki/20170101/) . This data gives a snapshot of wiki pages and their connection to other pages. Compressed `pagelinks` and `pages` sql dump are of 5GB and 1.5GB in size respectively, which provides a challenge if you plan to merge these tables over your SQL server (don't do it).

Secondly, we need metadata that we are interested in analyzing. Wiki media also provides `pageviews` dataset that tells us the count of landings on each wiki page for every hour of the day (data is available for 2015+), which is amazing. To save space, folks at wiki media save timestamp information on the filename. Unlike `pages` and `pagelinks`, `pageviews` dataset is saved in compressed text format and each dataset is ~50 MB in size (x10 after uncompress). This also means that if you plan to do some time-series analysis on `pageview` counts, you will have to transform data by adding one extra timestamp column and perform a join operation with files with the different timestamp, each of which are ~700 MB in size. This is obviously a costly calculation and requires some distributed solution.

For my use case, I converted data to a `.json` format that `Spark` understands before sending to `S3`. I then ingest this data with 4 Spark worker clusters, where I do map-reduce operations before saving to `Cassandra`

After some transformations and parsing of tables, sample 

#### Pagelinks

    6799,0,"""Hello,_World!""_program",0
    11012,0,"""Hello,_World!""_program",0




#### Pages

    7705,0,Cartesian_coordinates,,13,1,0,0.72461493194533,20160902065705,,528536176,41,wikitext
    7719,1,Chinese_remainder_theorem,,115,0,0,0.880412228294101,20160902002026,20160902002027,737313357,42628,wikitext

#### Pageviews

Spark's json read function expects each json object to be in a new line:

    {"title": "Pressure_system", "vcount": "10", "ymdh": "2016-12-01-000000", "prj": "en"}
    {"title": "Atmospheric_thermodynamics", "vcount": "4", "ymdh": "2016-12-01-000000", "prj": "en"}

Data transformation code is found [here](link to code)

## AWS Cluster

[Back to Table of Contents] (README.md#table-of-contents)

Configurations files to spin up AWS clusters can be found [here](conf/). I spawned 5 `Spark` clusters with 4 workers and 4 separate `Cassandra` clusters. Having `Cassandra` on the same node as `Spark`, as it turns out,  is not quite desirable for my use case primarily because I needed the memory for map-reduce operations. Furthermore, having them on separate clusters lets you separate issues, and not loose data in case something happens to your `Spark` clusters. 

---



## Batch Processing

[Back to Table of Contents] (README.md#table-of-contents)

The processed data sits on S3 bucket. I used Spark `sqlContext.read.json` and converted it to rdd before making other transformations. It is best to persist data in memory/disk after ingesting S3 data if you're doing further transformations down the line. Code for batch-processing can be found [here](ingest/s3_spark_json.py).

---



## Cassandra

[Back to Table of Contents] (README.md#table-of-contents)

Unlike relational databases, you typically think about what queries you are interested in before designing your database schema fro `Cassandra`. The data pipeline currently stores hourly, daily, and graph data (up to 2 degrees) and the schema for these tables can be found [here](batch/create_tables.py).

---


## Flask

[Back to Table of Contents] (README.md#table-of-contents)

The front end for my web application supports queries to the result tables residing on `Cassandra`. Users can enter wiki page and get up to 2 degrees away from the parent page, enter `date` and get wiki pages with the highest number of views. Furthermore, users can enter multiple pages to get pageview counts for those pages for comparison [website](http://wikiview.site/).
