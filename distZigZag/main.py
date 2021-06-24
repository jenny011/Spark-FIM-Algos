from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *

from operator import add
import os, math, json
import numpy as np
import pandas as pd

from algo import *
from utils import *
import threading


def local_zigzag(db_id, db, min_sup, vdbPath):
    zigzag_instance = ZigZag(min_sup, len(db), db_id)
    zigzag_instance.genVDB(db)
    zigzag_instance.prepStates()
    zigzag_instance.run()
    zigzag_instance.updateRetainedFIs()
    zigzag_instance.saveVDB(vdbPath)
    return zigzag_instance


def zigzag(dbPath, min_sup, sc, partition, minsup, vdbPath):
    # prep: read db
    dbFile = sc.textFile(dbPath)
    dbSize = dbFile.count()
    db = dbFile.map(lambda r: r.split(" ")).collect()

    # step 1: split dataset ???? Use Grouping ????
    # ???? WARN TaskSetManager: Stage 2 contains a task of very large size (4178 KB). The maximum recommended task size is 100 KB.
    partition_size = math.ceil(dbSize / partition)
    dbGroupMap = [(i, db[ i * partition_size : min( (i+1) * partition_size, dbSize )]) for i in range(partition)]
    dbGroup = sc.parallelize(dbGroupMap)

    # step 2: local mfis -- parallel ZigZag
    local_zigzags = dbGroup.map(lambda db_shard: local_zigzag(db_shard[0], db_shard[1], min_sup, vdbPath+"_"+str(db_shard[0])+".json")).cache()

    # step 3: union of local mfis
    all_localMFIs = set()
    for shard in local_zigzags.collect():
        for mfi in shard.mfis:
            all_localMFIs.add(",".join(mfi))
    all_localMFIs = list(all_localMFIs)
    for i in range(len(all_localMFIs)):
        all_localMFIs[i] = all_localMFIs[i].split(",")

    # step 4: generate local fis
    global_fis = local_zigzags\
                .flatMap(lambda zigzag_instance: zigzag_instance.all_powersets(all_localMFIs))\
                .reduceByKey(lambda a, b: a + b)\
                .filter(lambda kv: kv[1] >= minsup).collect()

    # step 5: filter global fis
    # global_fis = local_fis.filter(lambda kv: kv[1] >= minsup).collect()

    return local_zigzags, global_fis



def local_zigzagInc(zigzag_instance, incDB, vdbPath):
    vIncDB = transposeDB(incDB, zigzag_instance.dbSize)
    zigzag_instance.getVDB(vdbPath)
    zigzag_instance.updateStates(vIncDB, len(incDB))
    zigzag_instance.runInc()
    zigzag_instance.updateRetainedFIs()
    zigzag_instance.saveVDB(vdbPath)
    return zigzag_instance


def zigzagInc(incDBPath, min_sup, sc, partition, minsup, local_zigzags, vdbPath):
    # prep: read db
    dbFile = sc.textFile(incDBPath)
    dbSize = dbFile.count()
    db = dbFile.map(lambda r: r.split(" ")).collect()

    # step 1: split dataset ???? Use Grouping ????
    partition_size = math.ceil(dbSize / partition)
    dbGroupMap = [(i, db[ i * partition_size : min( (i+1) * partition_size, dbSize )]) for i in range(partition)]

    local_zigzags = local_zigzags.map(lambda shard: local_zigzagInc(shard, dbGroupMap[shard.gid][1], vdbPath+"_"+shard.gid+".json")).cache()

    # step 3: union of local mfis
    all_localMFIs = set()
    for shard in local_zigzags.collect():
        for mfi in shard.mfis:
            all_localMFIs.add(",".join(mfi))
    all_localMFIs = list(all_localMFIs)
    for i in range(len(all_localMFIs)):
        all_localMFIs[i] = all_localMFIs[i].split(",")

    # step 4: generate local fis
    global_fis = local_zigzags\
                .flatMap(lambda zigzag_instance: zigzag_instance.all_powersets(all_localMFIs))\
                .reduceByKey(lambda a, b: a + b)\
                .filter(lambda kv: kv[1] >= minsup).collect()

    # step 5: filter global fis
    # global_fis = local_fis.filter(lambda kv: kv[1] >= minsup).collect()

    return local_zigzags, global_fis
