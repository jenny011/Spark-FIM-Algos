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


def pfp(db, min_sup, sc, partition, minsup, resultPath):
    # step 1 & 2: sharding and parallel counting
    FlistRDD = db.flatMap(lambda trx: [(k,1) for k in trx])\
                    .reduceByKey(add)\
                    .filter(lambda kv: kv[1] >= minsup)\
                    .sortBy(lambda kv: kv[1], False)

    # 'hdfs://master.hadoop:7077/data/'
    FMap = {}
    for kv in FlistRDD.collect():
        FMap[kv[0]] = kv[1]
    # writeFMapToJSON(FMap, './data/flist.json')

    Flist = FlistRDD.map(lambda kv: kv[0])\
                    .collect()

    # step 3: Grouping items
    itemGidMap = {}
    gidItemMap = {}
    for item in Flist:
        gid = groupID(int(item), partition)
        itemGidMap[item] = gid
        gidItemMap[gid] = gidItemMap.get(gid, []) + [item]

    # step 4: pfp
    # Mapper – Generating group-dependent transactions
    groupDB = db.map(lambda trx: sortByFlist(trx, Flist))\
                .flatMap(lambda trx: groupDependentTrx(trx, itemGidMap))\
                .groupByKey()\
                .map(lambda kv: (kv[0], list(kv[1])))

    # Reducer – FP-Growth on group-dependent shards
    # localFIs = groupTrans.flatMap(lambda condDB: fpg(condDB[0], condDB[1], minsup, gidItemMap)).collect()
    localFIs = groupDB.flatMap(lambda condDB: buildAndMine(condDB[0], condDB[1], minsup))

    # step 5: Aggregation - remove duplicates
    globalFIs = set(localFIs.collect())
    print("result>>>", globalFIs)
    with open(resultPath, 'w') as f:
        json.dump(list(globalFIs), f)

    return FMap, itemGidMap, gidItemMap


def incPFP(db, min_sup, sc, partition, incDBPath, minsup, resultPath, FMap, itemGidMap, gidItemMap):
    # prep: read deltaD
    deltaDBFile = sc.textFile(incDBPath)
    deltaDBSize = deltaDBFile.count()
    deltaDB = deltaDBFile.map(lambda r: r.split(" "))

    newDB = sc.union([db, deltaDB])

    # step 1: Inc-Flist
    incFlistRDD = deltaDB.flatMap(lambda trx: [(k,1) for k in trx])\
                    .reduceByKey(add)\
                    .filter(lambda kv: kv[1] >= min_sup * deltaDBSize)\
                    .sortBy(lambda kv: kv[1], False)
    print("incFlist>>>", incFlistRDD.collect())

    incFlist = incFlistRDD.map(lambda kv: kv[0])\
                    .collect()

    # merge Inc-Flist and Flist
    # FMap = readFlistFromJSON('./data/flist.json')
    for kv in incFlistRDD.collect():
        if kv[0] in FMap:
            FMap[kv[0]] = FMap[kv[0]] + kv[1]
        else:
            FMap[kv[0]] = kv[1]

    # writeFMapToJSON(FMap, './data/flist.json')
    print("newFlist>>>", FMap)
    Flist = list(FMap.keys())

    # step 2: shard new DB
    for item in incFlist:
        gid = groupID(int(item), partition)
        itemGidMap[item] = gid
        gidItemMap[gid] = gidItemMap.get(gid, []) + [item]

    groupDB = newDB.map(lambda trx: sortByFlist(trx, Flist))\
                    .flatMap(lambda trx: groupDependentTrx(trx, itemGidMap))\
                    .groupByKey()\
                    .map(lambda kv: (kv[0], list(kv[1])))

    # step 3: mine new FP-tree using Inc-Flist
    localFIs = groupDB.flatMap(lambda condDB: checkBuildAndMine(incFlist, gidItemMap[condDB[0]], condDB[0], condDB[1], minsup))
    globalFIs = set(localFIs.collect())

    # load old results, merge, save
    with open(resultPath, 'r') as f:
        oldFIs = json.load(f)

    mergedFIs = globalFIs.union(oldFIs)
    print("mergedResult>>>",mergedFIs)
    with open(resultPath, 'w') as f:
        json.dump(list(mergedFIs), f)

    return newDB, FMap, itemGidMap, gidItemMap
