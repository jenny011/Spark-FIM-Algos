from pyspark import RDD, SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *

import os, json, argparse, math

from main import pfp
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
    # --------------- shared MACROS ----------------
    # --------------- shared MACROS ----------------
    dbdir = "hdfs:///incdatasets"
    database = args.database
    support = args.support
    partition = args.partition
    interval = args.interval

    # --------------- SPARK setup ----------------
    # --------------- SPARK setup ----------------
    conf = SparkConf().setAppName("PFP")
    sc = SparkContext.getOrCreate(conf=conf)

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

    # --------------- RUN exp ----------------
    db = None
    result = None
    oldFMap = None

    for inc_number in range(max_number):
        dbPath = os.path.join(dbdir, "interval_{0}_{1}/db_{2}.txt".format(database, interval, inc_number))
        res, db, oldFMap = pfp(dbPath, min_sup, sc, partition, minsup, oldFMap, db)
        if res:
            result = res

    # --------------- SAVE result ----------------
    print(result)

    sc.stop()
    return


if __name__=="__main__":
    main()
