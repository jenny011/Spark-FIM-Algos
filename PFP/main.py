from pyspark import RDD, SparkConf, SparkContext
from operator import add
import os, math, json

from fpGrowth import buildAndMine
from utils import *
import threading

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *


def pfp(dbPath, min_sup, sc, partition, minsup, flistPath, oldDB=None):
    # prep: read database
    dbList = scanDB(dbPath)
    dbSize = len(dbList)
    # !! cache
    db = sc.parallelize(dbList)

    # INC: merge DB
    if oldDB:
        # !! cache
        db = sc.union([db, oldDB])

    # step 1 & 2: sharding and parallel counting
    FlistRDD = db.flatMap(lambda trx: [(k,1) for k in trx])\
                    .reduceByKey(add)\
                    .filter(lambda kv: kv[1] >= minsup)\
                    .sortBy(lambda kv: kv[1], False)\
                    .collect()
    FMap = {}
    for kv in FlistRDD:
        FMap[kv[0]] = kv[1]
    Flist = list(FMap.keys())

    # INC: if Flist not changed, return
    if oldDB:
        # read old Flist
        with open(flistPath, 'r') as f:
            oldFMap = json.load(f)
        # compare
        skip = False
        if oldFlist is not None:
            skip = True
            for k, v in FMap.items():
                if k not in oldFMap or v != oldFMap[k]:
                    skip = False
                    break
        if skip:
            return None, db
        # output updated Flist
        with open(flistPath, 'w') as f:
            json.dump(FMap, f)
    else:
        with open(flistPath, 'w') as f:
            json.dump(FMap, f)

    # step 3: Grouping items
    itemGidMap = {}
    gidItemMap = {}
    for i in range(len(Flist)):
        gid = groupID(int(Flist[i]), partition)
        itemGidMap[Flist[i]] = gid
        gidItemMap[gid] = gidItemMap.get(gid, []) + [Flist[i]]


    # step 4: pfp
    # Mapper – Generating group-dependent transactions
    groupDB = db.map(lambda trx: sortByFlist(trx, FMap))\
                        .flatMap(lambda trx: groupDependentTrx(trx, itemGidMap))\
                        .groupByKey()\
                        .map(lambda kv: (kv[0], list(kv[1])))
    # Reducer – FP-Growth on group-dependent shards
    localFIs = groupDB.flatMap(lambda condDB: buildAndMine(condDB[0], condDB[1], minsup))

    # step 5: Aggregation - remove duplicates
    globalFIs = set(localFIs.collect())
    # print("result>>>", globalFIs)
    return globalFIs, db
