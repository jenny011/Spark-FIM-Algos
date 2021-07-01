from mpi4py import MPI
import numpy as np
import argparse
import json
import csv
import os
from time import time

from utils import * 

parser = argparse.ArgumentParser(description='argparse')
parser.add_argument('--dbpath', '-b', help='database path', required=True)
parser.add_argument('--database', '-d', help='database name', required=True)
parser.add_argument('--support', '-m', type=int, help='min support percentage', required=True)
parser.add_argument('--partition', '-p', type=int, help='num of workers', required=True)
parser.add_argument('--interval', '-i', help='interval', required=True)
args = parser.parse_args()

def main():
    dbdir = args.dbpath
    database = args.database
    support = args.support
    partition = args.partition
    interval = args.interval

    totalDB = get_DB_path("/gpfsnyu/home/jz2915/databases", database)

    minsup = calc_minsup(support, totalDB)

    me = worker(minsup)

    inc_number = 0
    dbPath = os.path.join(dbdir, "interval_{0}_{1}/db_{2}.txt".format(database, interval, inc_number))

    while os.path.isfile(dbPath):
        # create new worker upon init
        db = scanDB(dbPath)
        # spanning
        for trx in db:
            if me._rank == 0:
                #input
                me.send(trx, partition)
                me.bcast_finish(partition)
            
            else:
                me.listening()

        inc_number += 1
        dbPath = os.path.join(dbdir, "interval_{0}_{1}/db_{2}.txt".format(database, interval, inc_number))
    return

if __name__=="__main__":
    main()
    
