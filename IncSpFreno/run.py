from pyspark import RDD, SparkConf, SparkContext
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from utils import *

import time
import os, argparse
import numpy as np

memory = '32g'
pyspark_submit_args = ' --driver-memory ' + memory + ' pyspark-shell'
os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args

parser = argparse.ArgumentParser(description='argparse')
parser.add_argument('--database', '-d', help='database name', required=True)
parser.add_argument('--partition', '-p', type=int, help='num of workers', required=True)
parser.add_argument('--interval', '-i', help='interval', required=True)
args = parser.parse_args()

def main():   
    database = args.database
    support = [1,11,21,31,41,51]
    partition = args.partition
    interval = args.interval
    
    conf = SparkConf().setAppName("")
    
    for s in support:
        for t in range(13):
            sc = SparkContext.getOrCreate(conf=conf)
                    
            transDataRaw = scanDB("./datasets/{}.txt".format(database), " ")
            numTrans = len(transDataRaw)
            minsup = (s/100) * numTrans

            incDir = "./incdatasets/interval_{0}_{1}".format(database,interval)
            incNames = os.listdir(incDir)

            freqRange = sc.parallelize(range(0, partition))
            freqRange = distFreno(os.path.join(incDir,"db_0.txt"), minsup, sc, partition, freqRange)

            for incName in incNames[1:]:
                freqRange = incFreno(os.path.join(incDir,incName), minsup, sc, partition, freqRange)
                #print(res)
            sc.stop()
    
    
if __name__=="__main__":
    main()
