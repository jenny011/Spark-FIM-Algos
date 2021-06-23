from pyspark import RDD, SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *

import os, argparse, time, json
import numpy as np

from main import pfp

memory = '10g'
pyspark_submit_args = ' --driver-memory ' + memory + ' pyspark-shell'
os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args
os.environ["PYTHONHASHSEED"]=str(232)
'''
parser = argparse.ArgumentParser(description='argparse')
parser.add_argument('--database', '-d', help='database name', required=True)
parser.add_argument('--minsup', '-m', help='min support percentage', required=True)
parser.add_argument('--partition', '-p', help='num of workers', required=True)
args = parser.parse_args()
'''

def main():
    database = ["retail"]
    support = [41]
    partition = 8

    conf = SparkConf().setAppName("PFP")
    conf.set("spark.default.parallelism", partition)
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

    for f in database:
        for s in support:
            dbPath = f"../datasets/{f}.txt"
            resultPath = f"./data/{s}/{partition}/result.json"
            
            res = pfp(dbPath, s/100, sc, partition)
            with open(resultPath, 'w') as f:
                json.dump(list(res), f)
            sc.stop()
    return


if __name__=="__main__":
    main()
