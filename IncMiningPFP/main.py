from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *

from operator import add
import os, math, json
import numpy as np

from fpGrowth import buildAndMine, checkBuildAndMine
from utils import *
import threading


def pfp(dbPath, min_sup, total_minsup, sc, partition, resultPath, flistPath):
    # prep: read database
    dbList = scanDB(dbPath)
    dbSize = len(dbList)
    # dbFile = sc.textFile(dbPath)
    # dbSize = dbFile.count()
    # minsup = min_sup * dbSize
    # db = dbFile.map(lambda r: r.split(" ")).cache()
    db = sc.parallelize(dbList).cache()

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
    # filter freq items
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
                .map(lambda condDB: buildAndMine(condDB[0], condDB[1], total_minsup))\
                .collect()

    # save result
    for i in range(len(globalFIs)):
        with open(resultPath + "_" + str(i) + ".json", 'w') as f:
            json.dump(globalFIs[i], f)

    return db, itemGidMap, gidItemMap, dbSize


def incPFP(db, min_sup, total_minsup, sc, partition, incDBPath, dbSize, resultPath, flistPath, itemGidMap, gidItemMap):
    # prep: read deltaD
    incDBList = scanDB(incDBPath)
    incDBSize = len(incDBList)
    incDB = sc.parallelize(incDBList)
    # incDBFile = sc.textFile(incDBPath)
    # incDBSize = incDBFile.count()
    # incDB = incDBFile.map(lambda r: r.split(" "))

    newDB = sc.union([db, incDB]).cache()
    # minsup = min_sup * (dbSize + incDBSize)

    # step 1: Inc-Flist, merge Inc-Flist and Flist
    incFlistKV = incDB.flatMap(lambda trx: [(k,1) for k in trx])\
                    .reduceByKey(add)\
                    .sortBy(lambda kv: kv[1], False)\
                    .collect()

    FMap = readFlistFromJSON(flistPath)
    incFMap = {}
    freqIncFMap = {}
    freqIncFlist = []
    for kv in incFlistKV:
        k = kv[0]
        v = kv[1]
        newv = FMap.get(k, 0) + v
        FMap[k] = newv
        incFMap[k] = v
        if newv >= total_minsup:
            freqIncFMap[k] = newv
            freqIncFlist.append(k)
    writeFMapToJSON(FMap, flistPath)
    incFlist = list(incFMap.keys())

    # step 2: shard new DB
    for item in incFlist:
        gid = groupID(int(item), partition)
        itemGidMap[item] = gid
        gidItemMap[gid] = gidItemMap.get(gid, []) + [item]

    globalFIs = newDB.map(lambda trx: sortByFlist(trx,freqIncFMap))\
                    .flatMap(lambda trx: groupDependentTrx(trx, itemGidMap))\
                    .groupByKey()\
                    .map(lambda kv: (kv[0], list(kv[1])))\
                    .map(lambda condDB: checkBuildAndMine(freqIncFlist, gidItemMap[condDB[0]], condDB[0], condDB[1], total_minsup))\
                    .collect()

    # merge results
    for i in range(len(globalFIs)):
        if globalFIs[i] is not False:
            with open(resultPath + "_" + str(i) + ".json", 'r') as f:
                try:
                    oldResults = json.load(f)
                except:
                    oldResults = []

            mergedResults = []
            for item in globalFIs[i]:
                if item not in oldResults:
                    mergedResults.append(item)
            mergedResults.extend(oldResults)

            with open(resultPath + "_" + str(i) + ".json", 'w') as f:
                json.dump(mergedResults, f)

    return newDB, itemGidMap, gidItemMap, dbSize + incDBSize
