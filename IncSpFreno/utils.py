from pyspark import RDD, SparkConf, SparkContext
import os
import numpy as np
import math

from tree import Tree, TreeNode
import threading

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import time



def scanDB(path, seperation):
    db = []
    f = open(path, 'r')
    for line in f:
        if line:
            temp_list = line.rstrip().split(seperation)
            temp_list = [int(i) for i in temp_list]
            temp_list.sort()
            temp_list = [str(i) for i in temp_list]
            db.append(temp_list)
    f.close()
    return db


def runFreno(transactions, minsup):
    # for each worker: input (transactions line, minsup) and return minsup list
    
    tree = Tree(minsup)
    for trx in transactions:
        tree.insert(tree._root,trx)
    return tree
    
def concatFreno(transactions, tree):
    # for each worker: input (transactions line, minsup) and return minsup list
    
    for trx in transactions:
        tree.insert(tree._root,trx)
    return tree

def incFreno(incDBPath, minsup, sc, k, freqRange, res):
    
    deltaDBRaw = scanDB(incDBPath, " ")
    
    
    out_rdd = []
    for trx in deltaDBRaw:
        out_rdd.extend([trx[i:] for i in range(len(trx))])
    transDataFile = sc.parallelize(out_rdd)
    transData = transDataFile.map(lambda v: (v[0], v))
    transData = transData.map(lambda v: v[1])
    transData = transData.groupBy(lambda v: int(v[0])%k).map(lambda v : (v[0], list(v[1]))).collect()
    
    
    res = freqRange.map(lambda v: concatFreno(transData[v][1],res[v])).collect()
    return freqRange, res
    
def distFreno(inFile, minsup, sc, k, freqRange):
    
    transDataRaw = scanDB(inFile, " ")
    
    out_rdd = []
    for trx in transDataRaw:
        out_rdd.extend([trx[i:] for i in range(len(trx))])
    transDataFile = sc.parallelize(out_rdd)    
    transData = transDataFile.map(lambda v: (v[0], v))
    transData = transData.map(lambda v: v[1])
    
    transData = transData.groupBy(lambda v: int(v[0])%k).map(lambda v : (v[0], list(v[1]))).collect()#.sortByKey()

    print("number of partitions used: {}".format(sc.defaultParallelism))

    #phase 3: Freno from k-itemsets
    res = freqRange.map(lambda v: runFreno(transData[v][1],minsup)).collect()
    return freqRange, res

