from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *

from operator import add
import os, math, json
import numpy as np
import pandas as pd

from fpGrowth import buildAndMine, checkBuildAndMine
from utils import *
import threading


def pfp(dbPath, min_sup, total_minsup, sc, partition, resultPath, flistPath):
    # prep: read database
    dbFile = sc.textFile(dbPath)
    dbSize = dbFile.count()
    minsup = min_sup * dbSize
    db = dbFile.map(lambda r: r.split(" ")).cache()

    # step 1 & 2: sharding and parallel counting
    Flist = db.flatMap(lambda trx: [(k,1) for k in trx])\
                    .reduceByKey(add)\
                    .sortBy(lambda kv: kv[1], False)\
                    .collect()
    # 'hdfs://master.hadoop:7077/data/'
    FMap = {}
    for kv in Flist:
        FMap[kv[0]] = kv[1]
    writeFMapToJSON(FMap, flistPath)
    freqFMap = {}
    for k, v in FMap.items():
        if v >= total_minsup:
            freqFMap[k] = v

    # step 3: Grouping items
    itemGidMap = {}
    gidItemMap = {}
    for item in freqFMap.keys():
        gid = groupID(int(item), partition)
        itemGidMap[item] = gid
        gidItemMap[gid] = gidItemMap.get(gid, []) + [item]

    # step 4: pfp
    # Mapper – Generating group-dependent transactions
    globalFIs = db.map(lambda trx: sortByFlist(trx, freqFMap))\
                .flatMap(lambda trx: groupDependentTrx(trx, itemGidMap))\
                .groupByKey()\
                .map(lambda kv: (kv[0], list(kv[1])))\
                .flatMap(lambda condDB: buildAndMine(condDB[0], condDB[1], total_minsup))\
                .collect()

    # Reducer – FP-Growth on group-dependent shards
    # localFIs = groupDB.flatMap(lambda condDB: buildAndMine(condDB[0], condDB[1], total_minsup))

    # step 5: Aggregation - remove duplicates
    # globalFIs = localFIs.reduceByKey(add).collect()

    # result = {}
    # for kv in globalFIs:
    #     result[kv[0]] = result.get(kv[0], 0) + kv[1]
    # globalFIs = set(localFIs.collect())
    # print("result>>>", result)
    # with open(resultPath, 'w') as f:
    #     json.dump(result, f)

    return db, itemGidMap, gidItemMap, dbSize, set(globalFIs)

## DEBUG: {'265', '118', '1328,49', '32'}
def incPFP(db, min_sup, total_minsup, sc, partition, incDBPath, dbSize, resultPath, flistPath, itemGidMap, gidItemMap):
    # prep: read deltaD
    incDBFile = sc.textFile(incDBPath)
    incDBSize = incDBFile.count()
    incDB = incDBFile.map(lambda r: r.split(" "))

    newDB = sc.union([db, incDB]).cache()
    minsup = min_sup * (dbSize + incDBSize)

    # step 1: Inc-Flist, merge Inc-Flist and Flist
    incFlistKV = incDB.flatMap(lambda trx: [(k,1) for k in trx])\
                    .reduceByKey(add)\
                    .sortBy(lambda kv: kv[1], False)\
                    .collect()

    FMap = readFlistFromJSON(flistPath)
    incFMap = {}
    for kv in incFlistKV:
        k = kv[0]
        v = kv[1]
        FMap[k] = FMap.get(k, 0) + v
        incFMap[k] = v
    writeFMapToJSON(FMap, flistPath)
    incFlist = list(incFMap.keys())

    # step 2: shard new DB
    for item in incFlist:
        gid = groupID(int(item), partition)
        itemGidMap[item] = gid
        gidItemMap[gid] = gidItemMap.get(gid, []) + [item]

    globalFIs = newDB.map(lambda trx: sortByFlist(trx, incFMap))\
                    .flatMap(lambda trx: groupDependentTrx(trx, itemGidMap))\
                    .groupByKey()\
                    .map(lambda kv: (kv[0], list(kv[1])))\
                    .flatMap(lambda condDB: checkBuildAndMine(incFlist, gidItemMap[condDB[0]], condDB[0], condDB[1], total_minsup))\
                    .collect()

    # result = {}
    # for kv in globalFIs:
    #     result[kv[0]] = result.get(kv[0], 0) + kv[1]

    # step 3: mine new FP-tree using Inc-Flist
    # localFIs = groupDB.flatMap(lambda condDB: checkBuildAndMine(incFlist, gidItemMap[condDB[0]], condDB[0], condDB[1], total_minsup))

    # aggregate
    # globalFIs = localFIs.reduceByKey(add).collect()
    # globalFIs = set(localFIs.collect())

    # step 4: load old results, merge, save
    # with open(resultPath, 'r') as f:
    #     oldFIs = json.load(f)

    # for kv in globalFIs:
    #     oldFIs[kv[0]] = kv[1]
    # result = {}
    # for k, v in oldFIs.items():
    #     if v >= minsup:
    #         result[k] = v

    # mergedFIs = globalFIs.union(oldFIs)
    # print("mergedResult>>>",oldFIs.keys())
    # with open(resultPath, 'w') as f:
    #     json.dump(oldFIs, f)

    return newDB, itemGidMap, gidItemMap, dbSize + incDBSize, set(globalFIs)
