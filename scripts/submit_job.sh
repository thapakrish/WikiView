spark-submit \
    --master spark://ip-172-31-1-106:7077 \
    --executor-memory 5G \
    --driver-memory 5G \
    --packages TargetHolding/pyspark-cassandra:0.3.5 \
    --conf spark.cassandra.connection.host=172.31.1.100,172.31.1.107,172.31.1.104,172.31.1.103,172.31.1.101,172.31.1.110,172.31.1.109,172.31.1.108 \
    batch/joinpages.py
