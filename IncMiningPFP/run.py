from pyspark import RDD, SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *

import os, json, argparse

from main import pfp, incPFP
from utils import countDB

# memory = 32
# pyspark_submit_args = f' --driver-memory {memory}g pyspark-shell'
# pyspark_submit_args = f' --executor-memory {memory}g pyspark-shell'
# os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args
# os.environ["PYTHONHASHSEED"]=str(232)

parser = argparse.ArgumentParser(description='argparse')
parser.add_argument('--database', '-d', help='database name', required=True)
parser.add_argument('--support', '-m', type=int, help='min support percentage', required=True)
parser.add_argument('--partition', '-p', type=int, help='num of workers', required=True)
parser.add_argument('--interval', '-i', help='interval', required=True)
args = parser.parse_args()

def main():
    # --------------------- shared MACROS ---------------------
    # --------------------- shared MACROS ---------------------
    dbdir = "./incdatasets"
    database = args.database
    support = args.support
    partition = args.partition
    # interval = 0: no increment, mine the whole database
    interval = args.interval
    # test_num = 1

    # for t in range(test_num):
    # --------------------- SPARK setup ---------------------
    # --------------------- SPARK setup ---------------------
    conf = SparkConf().setAppName("IncMiningPFP")
    # conf.set("spark.default.parallelism", str(partition))
    sc = SparkContext.getOrCreate(conf=conf)
    # sc.setLogLevel("INFO")

    spark = SparkSession(sc)
    schema = StructType([
        StructField("algorithm", StringType(), False),
        StructField("datasets", StringType(), False),
        StructField("support", FloatType(), False)
    ])
    for i in range(1):
        schema.add("test{}".format(i+1), FloatType(), True)

    # --------------- EXPERIMENTS ----------------
    # --------------- EXPERIMENTS ----------------

    # --------------- exp MACROS ----------------
    min_sup = support/100

    dbSize = countDB(dbdir, database, interval)
    minsup = min_sup * dbSize

    # resultPath = f"./data/{database}_{support}_{interval}_{partition}/result.json"
    # flistPath = f"./data/{database}_{support}_{interval}_{partition}/flist.json"
    resultPath = "./data/result"
    flistPath = "./data/flist.json"

    # --------------- RUN exp ----------------
    # --- base ---
    inc_number = 0
    dbPath = os.path.join(dbdir, "interval_{0}_{1}/db_{2}.txt".format(database, interval, inc_number))
    db, itemGidMap, gidItemMap, dbSize = pfp(dbPath, min_sup, minsup, sc, partition, resultPath, flistPath)

    # --- increment ---
    inc_number += 1
    incDBPath = os.path.join(dbdir, "interval_{0}_{1}/db_{2}.txt".format(database, interval, inc_number))
    while os.path.isfile(incDBPath):
        db, itemGidMap, gidItemMap, dbSize = incPFP(db, min_sup, minsup, sc, partition, incDBPath, dbSize, resultPath, flistPath, itemGidMap, gidItemMap)
        inc_number += 1
        incDBPath = os.path.join(dbdir, "interval_{0}_{1}/db_{2}.txt".format(database, interval, inc_number))

    # --- SAVE ---
    result = []
    for i in range(partition):
        with open(resultPath + "_" + str(i) + ".json", "r") as f:
            try:
                partialResult = json.load(f)
                result.extend(partialResult)
            except:
                continue
    print(set(result))
    ##### !!! comment it out to test SPEED !!! #####
    ##### !!! comment it out to test SPEED !!! #####
    # with open(resultPath, 'w') as f:
    #     json.dump(list(result), f)
    ##### !!! comment it out to test SPEED !!! #####
    ##### !!! comment it out to test SPEED !!! #####
    sc.stop()
    return


if __name__=="__main__":
    main()
