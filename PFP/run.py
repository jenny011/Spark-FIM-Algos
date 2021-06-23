from pyspark import RDD, SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *

import os, argparse, time, json
import numpy as np

from main import pfp
from utils import countDB

memory = '10g'
pyspark_submit_args = ' --driver-memory ' + memory + ' pyspark-shell'
os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args
os.environ["PYTHONHASHSEED"]=str(232)

# parser = argparse.ArgumentParser(description='argparse')
# parser.add_argument('--database', '-d', help='database name', required=True)
# parser.add_argument('--minsup', '-m', help='min support percentage', required=True)
# parser.add_argument('--partition', '-p', help='num of workers', required=True)
# args = parser.parse_args()

def main():
    dbdir = "../incdatasets"
    database = "retail"
    support = 40
    min_sup = support/100
    partition = 3
    # interval = 0: no increment, mine the whole database
    interval = 0

    dbSize = countDB(dbdir, database, interval)
    minsup = min_sup * dbSize

    resultPath = f"./data/{database}_{support}_{partition}_{interval}/result.json"

    conf = SparkConf().setAppName("PFP")
    conf.set("spark.default.parallelism", str(partition))
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

    #for f in testFiles:
    #for s in support:
    # dbPath = f"../datasets/{database}.txt"
    # resultPath = f"./data/{args.minsup}/{args.partition}/result.json"
    result = None

    inc_number = 0
    dbPath = os.path.join(dbdir, f"interval_{database}_{interval}/db_{inc_number}.txt")
    res, db, flist = pfp(dbPath, min_sup, sc, partition, minsup)
    result = res
    print(result)

    inc_number += 1
    incDBPath = os.path.join(dbdir, f"interval_{database}_{interval}/db_{inc_number}.txt")
    while os.path.isfile(incDBPath):
        res, db, flist = pfp(incDBPath, min_sup, sc, partition, minsup, db, flist)
        if res:
            result = res
        print(result)
        inc_number += 1
        incDBPath = os.path.join(dbdir, f"interval_{database}_{interval}/db_{inc_number}.txt")

    with open(resultPath, 'w') as f:
        json.dump(list(results), f)
    sc.stop()
    return


if __name__=="__main__":
    main()
