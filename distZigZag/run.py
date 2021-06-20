from pyspark import RDD, SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *

import os, argparse, time
import numpy as np

from algo import *
from utils import *
from main import zigzag, zigzagInc

memory = '10g'
pyspark_submit_args = ' --driver-memory ' + memory + ' pyspark-shell'
os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args
os.environ["PYTHONHASHSEED"]=str(232)

parser = argparse.ArgumentParser(description='argparse')
parser.add_argument('--dbdir', '-b', help='database directory', required=True)
parser.add_argument('--database', '-d', help='database name', required=True)
parser.add_argument('--min_sup', '-m', type=float, help='min support percentage', required=True)
parser.add_argument('--partition', '-p', help='num of workers', required=True)
parser.add_argument('--inc_number', '-i', type=int, help='number of increments', required=False, default=0)
parser.add_argument('--granularity', '-g', type=int, help='size of increment', required=False, default=0)
args = parser.parse_args()


def main():
    # --------------------- parameters ---------------------
    # --------------------- parameters ---------------------
    dbdir = args.dbdir
    database = args.database
    dbPath = get_dbPath(dbdir, database)

    min_sup = args.min_sup/100
    partition = int(args.partition)

    inc_number = args.inc_number
    granularity = args.granularity

    incDBPath = "../datasets/inctest.txt"
    resultPath = f"./data/{args.min_sup}/{args.partition}/result.json"


    # --------------------- spark setup ---------------------
    # --------------------- spark setup ---------------------
    conf = SparkConf().setAppName("IncMiningPFP")
    conf.set("spark.default.parallelism", args.partition)
    sc = SparkContext.getOrCreate(conf=conf)

    spark = SparkSession(sc)
    schema = StructType([
        StructField("algorithm", StringType(), False),
        StructField("datasets", StringType(), False),
        StructField("support", FloatType(), False)
    ])
    for i in range(1):
        schema.add("test{}".format(i+1), FloatType(), True)


    # --------------------- prep baseDB ---------------------
    # --------------------- prep baseDB ---------------------
    dbFile = sc.textFile(dbPath)
    dbSize = dbFile.count()
    minsup = min_sup * dbSize
    db = dbFile.map(lambda r: r.split(" ")).collect()

    inc_split = dbSize - inc_number * granularity
    assert(inc_split >= 0)

    # --------------------- run ---------------------
    # --------------------- run ---------------------
    local_zigzags = zigzag(db[:inc_split], dbSize, min_sup, sc, partition, minsup)

    for i in range(inc_number):
        incDB = db[inc_split + granularity * i : inc_split + granularity * (i+1)]
        local_zigzags = zigzagInc(incDB, granularity, min_sup, sc, partition, minsup, local_zigzags)
    sc.stop()
    return


if __name__=="__main__":
    main()
