'''FRENO'''
from treecen import *
from time import time
import sys, os, argparse


parser = argparse.ArgumentParser(description='argparse')
parser.add_argument('--dbpath', '-b', help='database path', required=True)
parser.add_argument('--database', '-d', help='database name', required=True)
parser.add_argument('--support', '-m', type=int, help='min support percentage', required=True)
parser.add_argument('--interval', '-i', help='interval', required=True)
args = parser.parse_args()

def scanDB(path):
	db = []
	f = open(path, 'r')
	for line in f:
		if line:
			db.append(line.rstrip().split(" "))
	f.close()
	return db



if __name__ == '__main__':
	dbdir = args.dbpath
	database = args.database
	support = args.support
	interval = args.interval

	totalDB = scanDB("/gpfsnyu/home/jz2915/databases/" + database + ".txt")
	minsup = support / 100 * len(totalDB)

	
	inc_number = 0
	dbPath = os.path.join(dbdir, "interval_{0}_{1}/db_{2}.txt".format(database, interval, inc_number))
	FrenoTree = Tree(minsup)
	
	while os.path.isfile(dbPath):
		db = scanDB(dbPath)
		for trx in db:
			trx.sort()
			FrenoTree.insert(FrenoTree._root, trx)

		inc_number += 1
		dbPath = os.path.join(dbdir, "interval_{0}_{1}/db_{2}.txt".format(database, interval, inc_number))

