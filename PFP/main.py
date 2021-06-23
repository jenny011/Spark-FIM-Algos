from pyspark import RDD, SparkConf, SparkContext
from operator import add
import os, math
import numpy as np

from fpGrowth import buildAndMine
from utils import *
import threading

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *


def pfp(dbPath, min_sup, sc, partition, minsup, oldDB=None, oldFlist=None):
    # prep: read database
    dbFile = sc.textFile(dbPath)
    dbSize = dbFile.count()
    db = dbFile.map(lambda r: r.split(" "))

    # inc
    if oldDB:
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
    # print("Flist>>>", Flist)

    # inc
    if oldFlist is not None and sorted(Flist) == sorted(oldFlist):
        return None, db, Flist

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
    # localFIs = groupTrans.flatMap(lambda condDB: fpg(condDB[0], condDB[1], minsup, gidItemMap)).collect()
    localFIs = groupDB.flatMap(lambda condDB: buildAndMine(condDB[0], condDB[1], minsup))

    # step 5: Aggregation - remove duplicates
    globalFIs = set(localFIs.collect())
    # print("result>>>", globalFIs)
    return globalFIs, db, Flist
