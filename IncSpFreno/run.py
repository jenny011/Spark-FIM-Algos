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
    testFiles = ["retail"]
    support = [1,11,21,31,41,51]
    partition = 8
    interval = [20000]

    conf = SparkConf().setAppName("")
    sc = SparkContext.getOrCreate(conf=conf)

    spark = SparkSession(sc)
    schema = StructType([
        StructField("algorithm", StringType(), False),
        StructField("datasets", StringType(), False),
        StructField("support", FloatType(), False)
    ])
    for i in range(1):
        schema.add("test{}".format(i+1), FloatType(), True)
    #experiments = []

    for f in testFiles:
        for s in support:
            for i in interval:
                for t in range(1):
                    transDataRaw = scanDB("./datasets/{}.txt".format(f), " ")
                    numTrans = len(transDataRaw)
                    minsup = (s/100) * numTrans

                    incDir = "./incdatasets/interval_{0}_{1}".format(f,i)
                    incNames = os.listdir(incDir)

                    freqRange = sc.parallelize(range(0, partition))
                    freqRange, res = distFreno(os.path.join(incDir,"db_0.txt"), minsup, sc, partition, freqRange)

                    for incName in incNames[1:]:
                        freqRange, res = incFreno(os.path.join(incDir,incName), minsup, sc, partition, freqRange, res)
                    print(res)
    sc.stop()
    
    
if __name__=="__main__":
    main()
