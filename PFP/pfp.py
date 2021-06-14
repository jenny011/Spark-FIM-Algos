from pyspark import RDD, SparkConf, SparkContext
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

def groupDependentTrx(trx, Glist):
    GTrxMap = {}
    for i in range(len(trx)):
        gid = Glist[trx[i]]
        GTrxMap[gid] = trx[:i+1]
    return [(k,v) for k, v in GTrxMap.items()]

def fpg(gid, db, minsup, GItemMap):
    print(gid, GItemMap[gid], db, minsup)
    return db

def pfp(inFile, min_sup, sc, partition):
    # prep: read database
    transDataFile = sc.textFile(inFile)
    numTrans = transDataFile.count()
    minsup = min_sup * numTrans
    transData = transDataFile.map(lambda r: r.split(" "))

    # step 1 & 2: sharding and parallel counting
    Flist = transData.flatMap(lambda trx: [(k,1) for k in trx])
                    .reduceByKey(+)
                    .filter(lambda kv: kv[1] >= minsup)
                    .sortBy(lambda kv: kv[1], False)
                    .map(lambda kv: kv[0])
                    .collect()
    print(Flist)

    # step 3: Grouping items
    Glist = {}
    GItemMap = {}
    for item in range(len(Flist)):
        gid = groupID(item, partition)
        Glist[Flist[item]] = gid
        GItemMap[gid] = Glist.get(gid, []) + [item]
    print(Glist, GItemMap)

    # step 4: pfp
    # Mapper – Generating group-dependent transactions
    groupTrans = transData.map(lambda trx: sortByFlist(trx, Flist))
                        .flatMap(lambda trx: groupDependentTrx(trx, Glist))
                        .groupByKey()
                        .map(lambda kv: (kv[0], list(kv[1])))
    # Reducer – FP-Growth on group-dependent shards
    print("group done")
    localFIs = groupTrans.flatMap(lambda condDB: fpg(condDB[0], condDB[1], minsup, GItemMap)).collect()
    print(localFIs)

    return localFIs
