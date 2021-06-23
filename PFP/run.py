from pyspark import RDD, SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *

import os, argparse, time, json
import numpy as np

from main import pfp
from utils import countDB

memory = '32g'
pyspark_submit_args = ' --driver-memory ' + memory + ' pyspark-shell'
os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args
# os.environ["PYTHONHASHSEED"]=str(232)

# parser = argparse.ArgumentParser(description='argparse')
# parser.add_argument('--database', '-d', help='database name', required=True)
# parser.add_argument('--minsup', '-m', help='min support percentage', required=True)
# parser.add_argument('--partition', '-p', help='num of workers', required=True)
# args = parser.parse_args()

def main():
    # --------------- shared MACROS ----------------
    # --------------- shared MACROS ----------------
    dbdir = "../incdatasets"
    databases = ["retail", "kosarak", "chainstore", "record"]
    supports = [1,3,5,7,9,10,20,30,40,50]
    partitions = [1,2,4,8,16]
    # interval = 0: no increment, mine the whole database
    intervals = [20000, 40000, 60000, 80000, 100000]
    test_num = 1


    for partition in partitions:
        # --------------- SPARK setup ----------------
        # --------------- SPARK setup ----------------
        conf = SparkConf().setAppName("PFP")
        # set partition number
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

        # --------------- EXPERIMENTS ----------------
        # --------------- EXPERIMENTS ----------------
        for database in databases:
            for support in supports:
                for interval in intervals:
                    print("......", database, support, partition, interval)
                    for t in range(test_num):
                        # --------------- exp MACROS ----------------
                        min_sup = support/100

                        dbSize = countDB(dbdir, database, interval)
                        minsup = min_sup * dbSize

                        resultPath = f"./data/{database}_{support}_{partition}_{interval}/result.json"
                        flistPath = f"./data/{database}_{support}_{partition}_{interval}/flist.json"

                        # --------------- RUN exp ----------------
                        result = None

                        # --- base ---
                        inc_number = 0
                        dbPath = os.path.join(dbdir, f"interval_{database}_{interval}/db_{inc_number}.txt")
                        res, db = pfp(dbPath, min_sup, sc, partition, minsup, flistPath)
                        result = res
                        #print(result)

                        # --- increment ---
                        inc_number += 1
                        incDBPath = os.path.join(dbdir, f"interval_{database}_{interval}/db_{inc_number}.txt")
                        while os.path.isfile(incDBPath):
                            res, db = pfp(incDBPath, min_sup, sc, partition, minsup, flistPath, db)
                            if res:
                                result = res
                            #print(result)
                            inc_number += 1
                            incDBPath = os.path.join(dbdir, f"interval_{database}_{interval}/db_{inc_number}.txt")

                        # --------------- SAVE result ----------------
                        ##### !!! comment it out to test SPEED !!! #####
                        ##### !!! comment it out to test SPEED !!! #####
                        with open(resultPath, 'w') as f:
                            json.dump(list(result), f)
                        ##### !!! comment it out to test SPEED !!! #####
                        ##### !!! comment it out to test SPEED !!! #####

        sc.stop()
    return


if __name__=="__main__":
    main()
