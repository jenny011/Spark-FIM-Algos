

/usr/local/hadoop/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --py-files archieves.zip --master spark://master.hadoop:7077 --conf spark.executorEnv.PYTHONHASHSEED=321 run.py >> output

