from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *

from operator import add
import os, math, json
import subprocess

from fpGrowth import buildAndMine, checkBuildAndMine
from utils import *
import threading

def pfp(dbPath, total_minsup, sc, partition, resultPath):
    # prep: read database
    # dbList = scanDB(dbPath)
    # dbSize = len(dbList)
    # db = sc.parallelize(dbList).cache()
    dbFile = sc.textFile(dbPath)
    dbSize = dbFile.count()
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
    # writeFMapToJSON(FMap, flistPath)
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
                .map(lambda condDB: (condDB[0], buildAndMine(condDB[0], condDB[1], total_minsup)))\
                .collect()

    for item in globalFIs:
        resRDD = sc.parallelize(item[1])
        resRDD.saveAsTextFile(resultPath + "_" + str(item[0]) + ".txt")

    # save result
    # for i in range(len(globalFIs)):
    #     with open(resultPath + "_" + str(i) + ".json", 'w') as f:
    #         json.dump(globalFIs[i], f)

    return db, itemGidMap, gidItemMap, dbSize, FMap


def incPFP(db, total_minsup, sc, partition, incDBPath, dbSize, resultPath, FMap, itemGidMap, gidItemMap):
    # prep: read deltaD
    # incDBList = scanDB(incDBPath)
    # incDBSize = len(incDBList)
    # incDB = sc.parallelize(incDBList)
    incDBFile = sc.textFile(incDBPath)
    incDBSize = incDBFile.count()
    incDB = incDBFile.map(lambda r: r.split(" ")).cache()
    newDB = sc.union([db, incDB]).cache()

    # step 1: Inc-Flist, merge Inc-Flist and Flist
    incFlistKV = incDB.flatMap(lambda trx: [(k,1) for k in trx])\
                    .reduceByKey(add)\
                    .sortBy(lambda kv: kv[1], False)\
                    .collect()

    # FMap = readFlistFromJSON(flistPath)
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
    # writeFMapToJSON(FMap, flistPath)
    incFlist = list(incFMap.keys())

    # step 2: shard new DB
    for item in incFlist:
        gid = groupID(int(item), partition)
        itemGidMap[item] = gid
        gidItemMap[gid] = gidItemMap.get(gid, []) + [item]

    condDBs = newDB.map(lambda trx: sortByFlist(trx,freqIncFMap))\
                    .flatMap(lambda trx: groupDependentTrx(trx, itemGidMap))\
                    .groupByKey()\
                    .map(lambda kv: (kv[0], list(kv[1]))).cache()

    oldResults = {}
    for item in condDBs.collect():
        try:
            oldResults[item[0]] = sc.textFile(resultPath + "_" + str(item[0]) + ".txt").collect()
        except:
            oldResults[item[0]] = []

    globalFIs = condDBs\
                    .map(lambda condDB: (condDB[0], checkBuildAndMine(oldResults[condDB[0]], freqIncFlist, gidItemMap[condDB[0]], condDB[0], condDB[1], total_minsup)))\
                    .collect()

    for item in globalFIs:
        if item[1] is not False:
            try:
                cmd = "hdfs dfs -rm -r {0}_{1}.txt".format(resultPath, str(item[0]))
                ret = subprocess.check_output(cmd, shell=True)
            except:
                pass

            resRDD = sc.parallelize(item[1])
            resRDD.saveAsTextFile(resultPath + "_" + str(item[0]) + ".txt")

    # merge results
    # for i in range(len(globalFIs)):
    #     if globalFIs[i] is not False:
    #         with open(resultPath + "_" + str(i) + ".json", 'r') as f:
    #             try:
    #                 oldResults = json.load(f)
    #             except:
    #                 oldResults = []
    #
    #         mergedResults = []
    #         for item in globalFIs[i]:
    #             if item not in oldResults:
    #                 mergedResults.append(item)
    #         mergedResults.extend(oldResults)
    #
    #         with open(resultPath + "_" + str(i) + ".json", 'w') as f:
    #             json.dump(mergedResults, f)

    return newDB, itemGidMap, gidItemMap, dbSize + incDBSize, FMap
