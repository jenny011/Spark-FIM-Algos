import os, math
import numpy as np

from fpGrowth import buildAndMine
import threading

def countDB(dbdir, database, interval):
    dbSize = 0
    expDBdir = os.path.join(dbdir, f"interval_{database}_{interval}")
    for filename in os.listdir(expDBdir):
        if filename.endswith(".txt"):
            with open(os.path.join(expDBdir, filename), 'r') as f:
                for line in f:
                    if line:
                        dbSize += 1
    return dbSize

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
    items = gidItemMap[gid]
    res = [item for item in items]
    for item in items:
        fi = buildAndMine(db, minsup)
        res += fi
    return res
