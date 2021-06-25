nohup /usr/local/hadoop/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --py-files archives.zip --master spark://master.hadoop:7077 --conf spark.executorEnv.PYTHONHASHSEED=321 --driver-memory 61g --conf spark.rpc.message.maxSize=1024 --conf spark.driver.maxResultSize=0 --conf spark.default.parallelism=16 run.py

python3 process_output.py
