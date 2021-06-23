from pyspark import RDD, SparkConf, SparkContext
from operator import add
import os, math
import numpy as np
import pandas as pd
import json

from fpGrowth import buildAndMine, checkBuildAndMine
from utils import *
import threading

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *


def pfp(dbPath, min_sup, sc, partition, resultPath, flistPath):
    # prep: read database
    dbFile = sc.textFile(dbPath)
    dbSize = dbFile.count()
    minsup = min_sup * dbSize
    db = dbFile.map(lambda r: r.split(" "))

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
        if v >= minsup:
            freqFMap[k] = v

    # step 3: Grouping items
    itemGidMap = {}
    gidItemMap = {}
    for item in FMap.keys():
        gid = groupID(int(item), partition)
        itemGidMap[item] = gid
        gidItemMap[gid] = gidItemMap.get(gid, []) + [item]

    # step 4: pfp
    # Mapper – Generating group-dependent transactions
    groupDB = db.map(lambda trx: sortByFlist(trx, freqFMap))\
                .flatMap(lambda trx: groupDependentTrx(trx, itemGidMap))\
                .groupByKey()\
                .map(lambda kv: (kv[0], list(kv[1])))

    # Reducer – FP-Growth on group-dependent shards
    # localFIs = groupTrans.flatMap(lambda condDB: fpg(condDB[0], condDB[1], minsup, gidItemMap)).collect()
    localFIs = groupDB.flatMap(lambda condDB: buildAndMine(condDB[0], condDB[1], minsup))

    # step 5: Aggregation - remove duplicates
    globalFIs = localFIs.reduceByKey(add).collect()

    result = {}
    for kv in globalFIs:
        result[kv[0]] = result.get(kv[0], 0) + kv[1]
    # globalFIs = set(localFIs.collect())
    print("result>>>", result)
    with open(resultPath, 'w') as f:
        json.dump(result, f)

    return db, itemGidMap, gidItemMap, dbSize

## DEBUG: {'265', '118', '1328,49', '32'}
def incPFP(db, min_sup, sc, partition, incDBPath, dbSize, resultPath, flistPath, itemGidMap, gidItemMap):
    # prep: read deltaD
    incDBFile = sc.textFile(incDBPath)
    incDBSize = incDBFile.count()
    incDB = incDBFile.map(lambda r: r.split(" "))

    newDB = sc.union([db, incDB])
    minsup = min_sup * (dbSize + incDBSize)

    # step 1: Inc-Flist, merge Inc-Flist and Flist
    incFlist = incDB.flatMap(lambda trx: [(k,1) for k in trx])\
                    .reduceByKey(add)\
                    .sortBy(lambda kv: kv[1], False)\
                    .collect()

    FMap = readFlistFromJSON(flistPath)
    freqIncFMap = {}
    for kv in incFlist:
        k = kv[0]
        v = kv[1]
        FMap[k] = FMap.get(k, 0) + v
        if v >= minsup:
            freqIncFMap[k] = v
    writeFMapToJSON(FMap, flistPath)

    # step 2: shard new DB
    for item in freqIncFMap:
        gid = groupID(int(item), partition)
        itemGidMap[item] = gid
        gidItemMap[gid] = gidItemMap.get(gid, []) + [item]

    groupDB = newDB.map(lambda trx: sortByFlist(trx, freqIncFMap))\
                    .flatMap(lambda trx: groupDependentTrx(trx, itemGidMap))\
                    .groupByKey()\
                    .map(lambda kv: (kv[0], list(kv[1])))

    # step 3: mine new FP-tree using Inc-Flist
    localFIs = groupDB.flatMap(lambda condDB: checkBuildAndMine(list(freqIncFMap.keys()), gidItemMap[condDB[0]], condDB[0], condDB[1], minsup))

    # aggregate
    globalFIs = localFIs.reduceByKey(add).collect()
    # globalFIs = set(localFIs.collect())

    # load old results, merge, save
    with open(resultPath, 'r') as f:
        oldFIs = json.load(f)

    for kv in globalFIs:
        oldFIs[kv[0]] = kv[1]

    result = {}
    for k, v in oldFIs.items():
        if v >= minsup:
            result[k] = v

    # mergedFIs = globalFIs.union(oldFIs)
    print("mergedResult>>>",result.keys())
    with open(resultPath, 'w') as f:
        json.dump(result, f)

    return newDB, itemGidMap, gidItemMap, dbSize + incDBSize
