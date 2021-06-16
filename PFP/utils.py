import os, math
import numpy as np

from fpGrowth import buildAndMine
import threading

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
