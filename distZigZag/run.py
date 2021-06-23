from pyspark import RDD, SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *

import os, json
import numpy as np

from algo import *
from utils import countDB
from main import zigzag, zigzagInc

memory = '32g'
pyspark_submit_args = ' --driver-memory ' + memory + ' pyspark-shell'
os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args
# os.environ["PYTHONHASHSEED"]=str(232)

# parser = argparse.ArgumentParser(description='argparse')
# parser.add_argument('--dbdir', '-b', help='database directory', required=True)
# parser.add_argument('--database', '-d', help='database name', required=True)
# parser.add_argument('--min_sup', '-m', type=float, help='min support percentage', required=True)
# parser.add_argument('--partition', '-p', help='num of workers', required=True)
# parser.add_argument('--inc_number', '-i', type=int, help='number of increments', required=False, default=0)
# parser.add_argument('--granularity', '-g', type=int, help='size of increment', required=False, default=0)
# args = parser.parse_args()


def main():
    # --------------------- shared MACROS ---------------------
    # --------------------- shared MACROS ---------------------
    dbdir = "../incdatasets"
    databases = ["retail", "kosarak", "chainstore", "record"]
    supports = [1,11,21,31,41,51]
    partitions = [1,2,4,8,16]
    # interval = 0: no increment, mine the whole database
    intervals = [0,20000,40000,60000,80000,100000]
    test_num = 1

    for partition in partitions:
        for database in databases:
            for support in supports:
                for interval in intervals:
                    print("......", database, support, partition, interval)
                    for t in range(test_num):
                        # --------------------- SPARK setup ---------------------
                        # --------------------- SPARK setup ---------------------
                        conf = SparkConf().setAppName("DistZigZag")
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

                        # --------------- exp MACROS ----------------
                        min_sup = support/100

                        dbSize = countDB(dbdir, database, interval)
                        minsup = min_sup * dbSize

                        resultPath = f"./data/{database}_{support}_{partition}_{interval}/result.json"
                        vdbPath = f"./data/{database}_{support}_{partition}_{interval}/vdb.json"

                        # --------------- RUN exp ----------------
                        # --- base ---
                        inc_number = 0
                        dbPath = os.path.join(dbdir, f"interval_{database}_{interval}/db_{inc_number}.txt")
                        local_zigzags, global_fis = zigzag(dbPath, min_sup, sc, partition, minsup, vdbPath)

                        # --- increment ---
                        inc_number += 1
                        incDBPath = os.path.join(dbdir, f"interval_{database}_{interval}/db_{inc_number}.txt")
                        while os.path.isfile(incDBPath):
                            local_zigzags, global_fis = zigzagInc(incDBPath, min_sup, sc, partition, minsup, local_zigzags, vdbPath)
                            inc_number += 1
                            incDBPath = os.path.join(dbdir, f"interval_{database}_{interval}/db_{inc_number}.txt")

                        # # --------------- AGGREGATE result ----------------
                        # # step 3: union of local mfis
                        # all_localMFIs = set()
                        # for zigzag_instance in local_zigzags.collect():
                        #     for mfi in zigzag_instance.mfis:
                        #         all_localMFIs.add(",".join(mfi))
                        #
                        # all_localMFIs = list(all_localMFIs)
                        # for i in range(len(all_localMFIs)):
                        #     all_localMFIs[i] = all_localMFIs[i].split(",")
                        #
                        # # step 4: generate local fis
                        # local_fis = local_zigzags\
                        #             .flatMap(lambda zigzag_instance: zigzag_instance.all_powersets(all_localMFIs))\
                        #             .reduceByKey(lambda a, b: a + b)
                        #
                        # # step 5: filter global fis
                        # global_fis = local_fis.filter(lambda kv: kv[1] >= minsup).collect()

                        # print(">>>", global_fis)
                        # --------------- SAVE result ----------------
                        ##### !!! comment it out to test SPEED !!! #####
                        ##### !!! comment it out to test SPEED !!! #####
                        for i in range(len(global_fis)):
                            global_fis[i] = global_fis[i][0]
                        with open(resultPath, 'w') as f:
                            json.dump(global_fis, f)
                        ##### !!! comment it out to test SPEED !!! #####
                        ##### !!! comment it out to test SPEED !!! #####
                        sc.stop()
    return


if __name__=="__main__":
    main()
