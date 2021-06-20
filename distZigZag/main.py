from pyspark import RDD, SparkConf, SparkContext
from operator import add
import os, math
import numpy as np
import pandas as pd
import json

from algo import *
from utils import *
import threading

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *

def local_zigzag(db_id, db, min_sup):
    zigzag_instance = ZigZag(min_sup * len(db), db, db_id)
    zigzag_instance.prep()
    zigzag_instance.run()
    zigzag_instance.updateRetainedFIs()
    return zigzag_instance


def zigzag(db, dbSize, min_sup, sc, partition, minsup):
    # step 1: split dataset ???? Use Grouping ????
    # WARN TaskSetManager: Stage 2 contains a task of very large size (4178 KB). The maximum recommended task size is 100 KB.
    partition_size = math.ceil(dbSize / partition)
    dbGroupMap = [(i, db[ i * partition_size : min( (i+1) * partition_size, dbSize )]) for i in range(partition)]
    dbGroup = sc.parallelize(dbGroupMap)
    # print("dbGroup partition number:", dbGroup.getNumPartitions())

    # step 2: local mfis -- parallel ZigZag
    local_zigzags = dbGroup.map(lambda db_shard: local_zigzag(db_shard[0], db_shard[1], min_sup))
    # print("local_zigzags>", local_zigzags.collect())

    # step 3: union of local mfis
    all_localMFIs = set()
    for shard in local_zigzags.collect():
        for mfi in shard.mfis:
            all_localMFIs.add(",".join(mfi))
    all_localMFIs = list(all_localMFIs)
    for i in range(len(all_localMFIs)):
        all_localMFIs[i] = all_localMFIs[i].split(",")
    # print("localMIFs>", all_localMFIs)

    # step 4: generate local fis
    # local_fis = local_zigzags\
    #             .flatMap(lambda zigzag_instance: zigzag_instance.all_powersets(all_localMFIs))\
    #             .reduceByKey(lambda a, b: a + b)
    # print("local_fis partition number:", local_fis.getNumPartitions())
    local_fis = local_zigzags\
            .flatMap(lambda zigzag_instance: dictToList(zigzag_instance.retained))\
            .reduceByKey(lambda a, b: a + b)

    # step 5: filter global fis
    global_fis = local_fis.filter(lambda kv: kv[1] >= minsup)
    print("global FIs>>>", global_fis.collect())
    return local_zigzags



def local_zigzagInc(zigzag_instance, incDB):
    zigzag_instance.update_incDB(incDB, len(zigzag_instance.db))
    zigzag_instance.runInc()
    zigzag_instance.updateRetainedFIs()
    return zigzag_instance


def zigzagInc(db, dbSize, min_sup, sc, partition, minsup, local_zigzags):
    # step 1: split dataset ???? Use Grouping ????
    partition_size = math.ceil(dbSize / partition)
    dbGroupMap = [(i, db[ i * partition_size : min( (i+1) * partition_size, dbSize )]) for i in range(partition)]

    local_zigzags = local_zigzags.map(lambda shard: local_zigzagInc(shard, dbGroupMap[shard.gid][1]))

    # step 3: union of local mfis
    all_localMFIs = set()
    for shard in local_zigzags.collect():
        for mfi in shard.mfis:
            all_localMFIs.add(",".join(mfi))
    all_localMFIs = list(all_localMFIs)
    for i in range(len(all_localMFIs)):
        all_localMFIs[i] = all_localMFIs[i].split(",")
    # print("localMIFs>", all_localMFIs)

    # step 4: generate local fis
    # local_fis = local_zigzags\
    #             .flatMap(lambda zigzag_instance: zigzag_instance.all_powersets(all_localMFIs))\
    #             .reduceByKey(lambda a, b: a + b)
    # print("local_fis partition number:", local_fis.getNumPartitions())
    local_fis = local_zigzags\
            .flatMap(lambda zigzag_instance: dictToList(zigzag_instance.retained))\
            .reduceByKey(lambda a, b: a + b)

    # step 5: filter global fis
    global_fis = local_fis.filter(lambda kv: kv[1] >= minsup)
    print("global FIs>>>", global_fis.collect())
    return local_zigzags

    return
