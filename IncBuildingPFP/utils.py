import os, math
import numpy as np
import pandas as pd
import json

from cantree import buildAndMine
import threading

def scanDB(fpath, delimiter):
    db = []
    with open(fpath,'r') as f:
        for line in f:
            if line:
                db.append(line.strip().split(delimiter))
    return db

def groupID(index, partition):
    return index % partition

def sortByFlist(trx, Flist):
    return sorted(trx, key = lambda i: Flist[i])

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

# def writeFlistToJSON(Flist, fpath):
#     Fdict = {'item':[], 'count':[]}
#     for kv in Flist:
#         Fdict['item'].append(kv[0])
#         Fdict['count'].append(kv[1])
#     df = pd.DataFrame(Fdict)
#     df.to_json(fpath)
#
# def readFlistFromJSON(fpath):
#     df = pd.read_json(fpath)
#     Flist = df.set_index('item')['count'].to_dict()
#     return Flist

def writeFlistToJSON(Flist, fpath):
    Fdict = {}
    for kv in Flist:
        Fdict[kv[0]] = kv[1]
    with open(fpath, 'w') as f:
        json.dump(Fdict, f)

def readFlistFromJSON(fpath):
    with open(fpath, 'r') as f:
        ret = json.load(f)
    return ret
