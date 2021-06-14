from pyspark import RDD, SparkConf, SparkContext
from operator import add
import os, math
import numpy as np

from fpGrowth import buildAndMine
import threading

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *

def groupID(index, partition):
    return index % partition

def sortByFlist(trx, Flist):
    # NO duplicate items in a trx
    sortedTrx = []
    for i in Flist:
        if i in trx:
            sortedTrx.append(i)
    return sortedTrx

def groupDependentTrx(trx, itemGidMap):
    GTrxMap = {}
    for i in range(len(trx)):
        gid = itemGidMap[trx[i]]
        GTrxMap[gid] = trx[:i+1]
    return [(k,v) for k, v in GTrxMap.items()]

def fpg(gid, db, minsup, gidItemMap):
    print(gid, gidItemMap[gid], db, minsup)
    items = gidItemMap[gid]
    res = [item for item in items]
    for item in items:
        fi = buildAndMine(db, minsup, item)
        res += fi
    return res

def pfp(inFile, min_sup, sc, partition):
    # prep: read database
    transDataFile = sc.textFile(inFile)
    numTrans = transDataFile.count()
    minsup = min_sup * numTrans
    transData = transDataFile.map(lambda r: r.split(" "))

    # step 1 & 2: sharding and parallel counting
    Flist = transData.flatMap(lambda trx: [(k,1) for k in trx])\
                    .reduceByKey(add)\
                    .filter(lambda kv: kv[1] >= minsup)\
                    .sortBy(lambda kv: kv[1], False)\
                    .map(lambda kv: kv[0])\
                    .collect()
    print("Flist:", Flist)

    # step 3: Grouping items
    itemGidMap = {}
    gidItemMap = {}
    for i in range(len(Flist)):
        gid = groupID(i, partition)
        itemGidMap[Flist[i]] = gid
        gidItemMap[gid] = gidItemMap.get(gid, []) + [Flist[i]]
    print("item-gid:", itemGidMap)
    print("gid-item:", gidItemMap)

    # step 4: pfp
    # Mapper – Generating group-dependent transactions
    groupTrans = transData.map(lambda trx: sortByFlist(trx, Flist))\
                        .flatMap(lambda trx: groupDependentTrx(trx, itemGidMap))\
                        .groupByKey()\
                        .map(lambda kv: (kv[0], list(kv[1])))
    print(groupTrans.collect())
    # Reducer – FP-Growth on group-dependent shards
    localFIs = groupTrans.flatMap(lambda condDB: fpg(condDB[0], condDB[1], minsup, gidItemMap)).collect()
    print(localFIs)

    return localFIs
