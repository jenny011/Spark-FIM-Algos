import sys, os, math
import numpy as np
import pandas as pd
import json, csv
import threading

# -------------------- db ops ----------------------
# -------------------- db ops ----------------------
def get_DB(DBDIR, dbname):
    if dbname == "retail":
        DBFILENAME = "retail.txt"
    elif dbname == "kosarak":
        DBFILENAME = "kosarak.txt"
    elif dbname == "chainstore":
        DBFILENAME = "chainstoreFIM.txt"
    elif dbname == "susy":
        DBFILENAME = "SUSY.txt"
    elif dbname == "record":
        DBFILENAME = "RecordLink.txt"
    elif dbname == "skin":
        DBFILENAME = "Skin.txt"
    elif dbname == "uscensus":
        DBFILENAME = "USCensus.txt"
    elif dbname == "online":
        DBFILENAME = "OnlineRetailZZ.txt"
    elif dbname == "test":
        DBFILENAME = "transData.txt"
    return scanDB(os.path.join(DBDIR, DBFILENAME), " ")

def get_dbPath(DBDIR, dbname):
    if dbname == "retail":
        DBFILENAME = "retail.txt"
    elif dbname == "kosarak":
        DBFILENAME = "kosarak.txt"
    elif dbname == "chainstore":
        DBFILENAME = "chainstoreFIM.txt"
    elif dbname == "susy":
        DBFILENAME = "SUSY.txt"
    elif dbname == "record":
        DBFILENAME = "RecordLink.txt"
    elif dbname == "skin":
        DBFILENAME = "Skin.txt"
    elif dbname == "uscensus":
        DBFILENAME = "USCensus.txt"
    elif dbname == "online":
        DBFILENAME = "OnlineRetailZZ.txt"
    elif dbname == "test":
        DBFILENAME = "transData.txt"
    return os.path.join(DBDIR, DBFILENAME)

def scanDB(fpath, delimiter):
    db = []
    with open(fpath, 'r') as f:
        for line in f:
            if line:
                trx = line.rstrip().split(delimiter)
                db.append(trx)
    return db

def transposeDB(hdb, base=0):
	vdb = {}
	for i in range(len(hdb)):
		for item in hdb[i]:
			if item in vdb:
				vdb[item].add(i + base)
			else:
				vdb[item] = {i + base}
	return vdb


# -------------------- db item ops ----------------------
# -------------------- db item ops ----------------------
def getDBItems(db):
	dbItems = {}
	for trx in db:
		for item in trx:
			dbItems[item] = dbItems.get(item, 0) + 1
	return dbItems

def getFreqDBItems(vdb, minsup):
    ret = {}
    for k, v in vdb.items():
        sup = len(v)
        if sup >= minsup:
            ret[k] = sup
    return ret


# -------------------- zigzag helper ----------------------
# -------------------- zigzag helper ----------------------
def ascOrderedList(fmap):
    flist = sorted(fmap, key=fmap.get)
    return flist

def mypowerset(l):
    if len(l) == 0:
        return {''}
    s = mypowerset(l[1:])
    t = set()
    for item in s:
        if item:
            temp = item.split(",") + [l[0]]
            temp.sort()
            t.add(",".join(temp))
        else:
            t.add(l[0])
    s = s.union(t)
    return s

def dictToList(d):
    ret = []
    for k, v in d.items():
        ret.append((k,v))
    return ret


# -------------------- partition ----------------------
# -------------------- partition ----------------------
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


# -------------------- IO ----------------------
# -------------------- IO ----------------------
def writeFMapToJSON(FMap, fpath):
    with open(fpath, 'w') as f:
        json.dump(FMap, f)

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
