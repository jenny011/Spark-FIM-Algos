from pyspark import RDD, SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *

import os, json, argparse, math

from main import pfp, incPFP
from utils import countDB

parser = argparse.ArgumentParser(description='argparse')
parser.add_argument('--database', '-d', help='database name', required=True)
parser.add_argument('--support', '-m', type=int, help='min support percentage', required=True)
parser.add_argument('--partition', '-p', type=int, help='num of workers', required=True)
parser.add_argument('--interval', '-i', help='interval', required=True)
args = parser.parse_args()


def main():
    # --------------------- shared MACROS ---------------------
    # --------------------- shared MACROS ---------------------
    dbdir = "hdfs:///incdatasets"
    database = args.database
    support = args.support
    partition = args.partition

    # --------------------- SPARK setup ---------------------
    # --------------------- SPARK setup ---------------------
    conf = SparkConf().setAppName("IncMiningPFP")
    # conf.set("spark.hadoop.validateOutputSpecs", "false")
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

    #dbSize = countDB(dbdir, database, interval)
    dbFile = sc.textFile(os.path.join(dbdir, "interval_{0}_0/db_0.txt".format(database)))
    dbSize = dbFile.count()
    minsup = min_sup * dbSize

    if int(interval) == 0:
        max_number = 1
    else:
        max_number = math.ceil(dbSize / int(interval))

    resultPath = "hdfs:///data/{0}_{1}_{2}_{3}".format(database, support, interval, partition)
    # flistPath = f"hdfs://master.hadoop:7077/data/{database}_{support}_{interval}_{partition}/flist.json"

    # --------------- RUN exp ----------------
    # --- base ---
    inc_number = 0
    dbPath = os.path.join(dbdir, "interval_{0}_{1}/db_{2}.txt".format(database, interval, inc_number))
    db, itemGidMap, gidItemMap, dbSize, FMap = pfp(dbPath, minsup, sc, partition, resultPath)

    # --- increment ---
    for inc_number in range(1, max_number):
        incDBPath = os.path.join(dbdir, "interval_{0}_{1}/db_{2}.txt".format(database, interval, inc_number))
        db, itemGidMap, gidItemMap, dbSize, FMap = incPFP(db, minsup, sc, partition, incDBPath, dbSize, resultPath, FMap, itemGidMap, gidItemMap)

    # --- SAVE ---
    result = []
    for i in gidItemMap.keys():
        res = sc.textFile(resultPath + "_" + str(i) + ".txt").collect()
        result.extend(res)
    print(set(result))

    sc.stop()
    return


if __name__=="__main__":
    main()
