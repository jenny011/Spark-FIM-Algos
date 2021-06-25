import os, math
import numpy as np

from fpGrowth import buildAndMine
import threading

def scanDB(fpath, delimiter=" "):
    db = []
    with open(fpath, 'r') as f:
        for line in f:
            if line:
                trx = line.rstrip().split(delimiter)
                db.append(trx)
    return db

def countDB(dbdir, database, interval):
    dbSize = 0
    expDBdir = os.path.join(dbdir, "interval_{0}_{1}".format(database, interval))
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
    temp = []
    for i in trx:
        if i in Flist:
            temp.append(i)
    return sorted(temp, key = lambda i: Flist[i])

def groupDependentTrx(trx, itemGidMap):
    GTrxMap = {}
    for i in range(len(trx)):
        gid = itemGidMap[trx[i]]
        GTrxMap[gid] = trx[:i+1]
    return [(k,v) for k, v in GTrxMap.items()]
