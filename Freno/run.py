from pyspark import RDD, SparkConf, SparkContext
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from utils import distFreno

import time
import os, argparse
import numpy as np

memory = '61g'
pyspark_submit_args = ' --driver-memory ' + memory + ' pyspark-shell'
os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args
#os.environ["PYTHONHASHSEED"]=str(232)

parser = argparse.ArgumentParser(description='argparse')
parser.add_argument('--database', '-d', help='database name', required=True)
parser.add_argument('--partition', '-p', type=int, help='num of workers', required=True)
args = parser.parse_args()

def main():
    
    database = args.database
    support = [1,11,21,31,41,51]
    partition = args.partition

    conf = SparkConf().setAppName("FrenoTest")
    #sc = SparkContext.getOrCreate(conf=conf)
    #sc.setLogLevel("INFO")
    #sc = SparkContext(conf=conf)

    #spark = SparkSession(sc)
    #schema = StructType([
    #    StructField("algorithm", StringType(), False),
    #    StructField("datasets", StringType(), False),
    #    StructField("support", FloatType(), False)
    #])
    #for i in range(1):
    #    schema.add("test{}".format(i+1), FloatType(), True)
    #experiments = []
    for s in support:
        for i in range(13):
            sc = SparkContext.getOrCreate(conf=conf)
            #sc.setLogLevel("INFO") 
            distFreno("./datasets/{}.txt".format(database), s/100, sc, partition)
            #print(res)
    
            sc.stop()
    return
    
    
if __name__=="__main__":
    main()
