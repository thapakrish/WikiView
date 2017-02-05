spark-submit \
    --master spark://ip-XXX-XX-X-XXX:7077 \
    --executor-memory 5G \
    --driver-memory 5G \
    --packages TargetHolding/pyspark-cassandra:0.3.5 \
    --conf spark.cassandra.connection.host=XXX.XX.X.X \
    batch/readjson.py
