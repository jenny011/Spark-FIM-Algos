DATABASE="retail"

for PARTITION in 4 8 16
do
    for INTERVAL in 20000 40000 60000 80000 100000
    do

        nohup /usr/local/hadoop/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --py-files archives.zip --master spark://master.hadoop:7077 --conf spark.executorEnv.PYTHONHASHSEED=321 --driver-memory 61g --conf spark.rpc.message.maxSize=1024 --conf spark.driver.maxResultSize=0 --conf spark.default.parallelism=$PARTITION run.py -d $DATABASE -p $PARTITION -i $INTERVAL

        python3 process_output.py -d $DATABASE -p $PARTITION -i $INTERVAL

        rm -rf nohup.out

    done
done