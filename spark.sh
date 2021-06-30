SPARK='/usr/local/hadoop/spark-2.1.0-bin-hadoop2.7'

sh $SPARK/sbin/stop-all.sh

sh $SPARK/sbin/start-master.sh

sh $SPARK/sbin/start-slaves.sh --SPARK_WORKER_CORES=1 -c=1

# UI
$SPARK/bin/pyspark --master spark://master.hadoop:7077

#公网:8080
#conf/spark-env.sh