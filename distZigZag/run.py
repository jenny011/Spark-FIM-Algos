from pyspark import RDD, SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *

import os, json, argparse
import numpy as np

from algo import *
from utils import countDB
from main import zigzag, zigzagInc

# memory = '32g'
# pyspark_submit_args = ' --driver-memory ' + memory + ' pyspark-shell'
# os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args
# os.environ["PYTHONHASHSEED"]=str(232)

# parser = argparse.ArgumentParser(description='argparse')
# parser.add_argument('--dbdir', '-b', help='database directory', required=True)
# parser.add_argument('--database', '-d', help='database name', required=True)
# parser.add_argument('--min_sup', '-m', type=float, help='min support percentage', required=True)
# parser.add_argument('--partition', '-p', help='num of workers', required=True)
# parser.add_argument('--inc_number', '-i', type=int, help='number of increments', required=False, default=0)
# parser.add_argument('--granularity', '-g', type=int, help='size of increment', required=False, default=0)
# args = parser.parse_args()
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
    conf = SparkConf().setAppName("DistZigZag")
    # set partition number
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
    # vdbPath = f"./data/{database}_{support}_{interval}_{partition}/vdb"
    resultPath = "./data/result.json"
    vdbPath = "./data/vdb"

    # --------------- RUN exp ----------------
    # --- base ---
    inc_number = 0
    dbPath = os.path.join(dbdir, "interval_{0}_{1}/db_{2}.txt".format(database, interval, inc_number))
    local_zigzags, global_fis = zigzag(dbPath, min_sup, sc, partition, minsup, vdbPath)

    # --- increment ---
    inc_number += 1
    incDBPath = os.path.join(dbdir, "interval_{0}_{1}/db_{2}.txt".format(database, interval, inc_number))
    while os.path.isfile(incDBPath):
        local_zigzags, global_fis = zigzagInc(incDBPath, min_sup, sc, partition, minsup, local_zigzags, vdbPath)
        inc_number += 1
        incDBPath = os.path.join(dbdir, "interval_{0}_{1}/db_{2}.txt".format(database, interval, inc_number))

    # --------------- SAVE result ----------------
    for i in range(len(global_fis)):
        global_fis[i] = global_fis[i][0]
    print(global_fis)
    ##### !!! comment it out to test SPEED !!! #####
    ##### !!! comment it out to test SPEED !!! #####
    # for i in range(len(global_fis)):
    #     global_fis[i] = global_fis[i][0]
    # with open(resultPath, 'w') as f:
    #     json.dump(global_fis, f)
    ##### !!! comment it out to test SPEED !!! #####
    ##### !!! comment it out to test SPEED !!! #####
    sc.stop()
    return


if __name__=="__main__":
    main()
