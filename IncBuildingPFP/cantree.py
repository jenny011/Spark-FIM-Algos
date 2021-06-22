'''CanTree'''
import tree as myTree
from FP_Growth import fpGrowth
from time import time
import sys


#----------scan the db-----------
def scanDB(path, separation):
	db = []
	f = open(path, 'r')
	for line in f:
		if line:
			db.append(line.rstrip().split(separation))
	f.close()
	return db

#-----------get item counts for a dataset---------
def getDBItems(db):
	dbItems = {}
	for trx in db:
		for item in trx:
			dbItems[item] = dbItems.get(item, 0) + 1
	return dbItems

#-----------build an CanTree-----------
def buildCanTree(db, dbItems):
	canTree = myTree.CanTree()
	canTree.createHeaderTable(dbItems)
	for trx in db:
		canTree.add(trx, 1)
	return canTree


#-----------mining-----------
#-----------mining-----------
#-----------mining-----------
#-----------mine an CanTree for an item-----------
def mine(tree, key, value, basePtn, minsup):
	basePtn += key + ','
	patterns = [basePtn]
	ptr = value
	condPB = []
	while ptr:
		ptn = tree.prefix_path(ptr)
		if ptn:
			condPB.append(ptn)
		ptr = ptr._next
	if len(condPB) > 0:
		condTree = fpGrowth.buildCondTree(condPB, minsup)
		patterns += fpGrowth.mineAll(condTree, minsup, basePtn)
	return patterns

#-----------mine an CanTree-----------
def mineAll(tree, dbItems, basePtn, minsup):
	allPatterns = []
	for key, value in tree.headerTable.items():
		if dbItems[key] >= minsup:
			allPatterns += mine(tree, key, value, basePtn, minsup)
	return allPatterns

def buildAndMine(gid, db, minsup, basePtn=''):
    dbItems = getDBItems(db)
    canTree = buildCanTree(db, dbItems, minsup)
    results = mineAll(canTree, dbItems, basePtn, minsup)
    return sorted(results)
