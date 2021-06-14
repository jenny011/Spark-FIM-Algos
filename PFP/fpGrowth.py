'''FP-Growth'''
import header as myHeader
import tree as myTree
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

#-----------build an fp-tree-----------
def buildFPTree(db, dbItems):
	fpTree = myTree.FPTree()
	fpTree.createHeaderTable(dbItems, minsup)
	for trx in db:
		fpTree.add(trx, 1)
	return fpTree

#-----------get item counts for a pattern base-----------
def getPBItems(pb):
	pbItems = {}
	for ptn in pb:
		for item in ptn[1]:
			pbItems[item] = pbItems.get(item, 0) + ptn[0]
	return pbItems

#-----------build a conditional fp-tree-----------
def buildCondTree(condPB):
	condTree = myTree.FPTree()
	pbItems = getPBItems(condPB)
	condTree.createHeaderTable(pbItems, minsup)
	for ptn in condPB:
		condTree.add(ptn[1], ptn[0])
	return condTree

#-----------mine an fp-tree for a pattern-----------
def mine(tree, header, basePtn):
	basePtn += header._key + ','
	patterns = [basePtn]
	ptr = header._next
	condPB = []
	while ptr:
		ptn = tree.prefix_path(ptr)
		if ptn:
			condPB.append(ptn)
		ptr = ptr._next
	if len(condPB) > 0:
		condTree = buildCondTree(condPB)
		patterns += mineAll(condTree, basePtn)
	return patterns

#-----------mine an fp-tree-----------
def mineAll(tree, basePtn):
	allPatterns = []
	for header in tree.headerTable.headers():
		allPatterns += mine(tree, header, basePtn)
	return allPatterns


def buildAndMine(db, f_perf=None):
	fpTree = buildFPTree(db, dbItems)
	results = mineAll(fpTree, '')
	return sorted(results)
