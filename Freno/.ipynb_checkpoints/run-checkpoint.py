from pyspark import RDD, SparkConf, SparkContext
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from utils import distFreno

import time
import os
import numpy as np

memory = '32g'
pyspark_submit_args = ' --driver-memory ' + memory + ' pyspark-shell'
os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args
#os.environ["PYTHONHASHSEED"]=str(232)

def main():
    
    database = ["kosarak"]
    support = [1,6,11,16,21,26,31,36,41,46]
    partition = 16

    conf = SparkConf().setAppName("FrenoTest")
    sc = SparkContext.getOrCreate(conf=conf)
    sc.setLogLevel("INFO")
    #sc = SparkContext(conf=conf)

    spark = SparkSession(sc)
    schema = StructType([
        StructField("algorithm", StringType(), False),
        StructField("datasets", StringType(), False),
        StructField("support", FloatType(), False)
    ])
    for i in range(1):
        schema.add("test{}".format(i+1), FloatType(), True)
    #experiments = []

    for f in database:
        for s in support:
            for i in range(13):
            	res = distFreno("./databases/{}.txt".format(f), s/100, sc, partition)
            	print(res)
    
    sc.stop()
    return res
    
    
if __name__=="__main__":
    main()
