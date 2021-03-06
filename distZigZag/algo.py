import sys, os, argparse
import json, csv
from time import time
from utils import *


class GenMax:
    def __init__(self, min_sup, dbSize):
        self.min_sup = min_sup
        ##### OPTIMIZE save dbSize instead of db
        self.dbSize = dbSize
        self.vdb = {}
        self.minsup = None
        self.mfis = []
        self.flist = []

    def genVDB(self, db):
        self.vdb = transposeDB(db)

    def get_tlist(self, itemset, itemsetStr):
        if itemsetStr not in self.vdb:
            if len(itemset) == 1 or itemset[0] not in self.vdb:
                self.vdb[itemset[0]] = set()
            for i in range(1, len(itemset)):
                temp = ",".join(itemset[:i])
                next = ",".join(itemset[:i+1])
                if next not in self.vdb:
                    self.vdb[next] = self.vdb[temp].intersection(self.vdb[itemset[i]])

    def getFlist(self):
        freqDBItems = getFreqDBItems(self.vdb, self.minsup)
        self.flist = ascOrderedList(freqDBItems)

    def isNotSubSetPosition(self, itemset, mfi):
        for i in range(len(itemset)):
            if itemset[i] not in mfi:
                return i + 1
        return -1

    def countItemsetVertical(self, itemset, itemsetStr, p):
        self.get_tlist(itemset, itemsetStr)
        new_tlist = self.vdb[itemsetStr].intersection(self.vdb[p])

        newItemsetStr = ",".join(sorted(itemset + [p]))
        self.vdb[newItemsetStr] = new_tlist
        return len(new_tlist)

    def fiCombineOrd(self, itemset, possibleSet):
        sortedItemset = sorted(itemset)
        itemsetStr = ",".join(sortedItemset)

        combineSetDict = {}
        for p in possibleSet:
            count = self.countItemsetVertical(sortedItemset, itemsetStr, p)

            if count >= self.minsup:
                combineSetDict[p] = count / (len(self.vdb[itemsetStr]) * len(self.vdb[p]))
        combineSet = ascOrderedList(combineSetDict)
        return combineSet

    def prepStates(self):
        self.minsup = self.min_sup * self.dbSize
        self.getFlist()

    def run(self):
        self.backTrack([], self.flist, self.mfis)
        self.sortMFIs()

    def backTrack(self, itemset, combineSet, mfis):
        for c in combineSet:
            newItemset = itemset + [c]
            newPossibleSet = combineSet[combineSet.index(c)+1 : ]
            next = False
            p = -1
            for i in range(len(mfis)):
                new_p = self.isNotSubSetPosition(newItemset + newPossibleSet, mfis[i])
                if new_p == -1:
                    next = True
                elif new_p > p:
                    p = new_p
            if next:
                return
            new_mfis = []
            newCombineSet = self.fiCombineOrd(newItemset, newPossibleSet)
            if not newCombineSet:
                if len(newItemset) >= p:
                    mfis.append(newItemset)
            else:
                new_mfis = [mfi for mfi in mfis if c in mfi]
                self.backTrack(newItemset, newCombineSet, new_mfis)
            for mfi in new_mfis:
                if mfi not in mfis:
                    mfis.append(mfi)

    def sortMFIs(self):
        for i in range(len(self.mfis)):
            self.mfis[i].sort()

    def generateFIs(self, mfis):
        freqItemsets = set()
        for mfi in mfis:
            freqItemsets = freqItemsets.union(mypowerset(mfi))
        freqItemsets.discard('')
        return freqItemsets


class ZigZag(GenMax):
    def __init__(self, min_sup, dbSize, gid):
        super().__init__(min_sup, dbSize)
        self.gid = gid
        self.vIncDB = {}
        self.retained = {}

    def get_tlistInc(self, itemset, itemsetStr):
        if itemsetStr not in self.vIncDB:
            if len(itemset) == 1 or itemset[0] not in self.vIncDB:
                self.vIncDB[itemset[0]] = set()
            for i in range(1, len(itemset)):
                temp = ",".join(itemset[:i])
                next = ",".join(itemset[:i+1])
                if next not in self.vIncDB:
                    self.vIncDB[next] = self.vIncDB[temp].intersection(self.vIncDB.get(itemset[i], set()))

    def support(self, itemsetStr, inc=False):
        itemset = itemsetStr.split(",")
        if inc:
            self.get_tlistInc(itemset, itemsetStr)
            return len(self.vIncDB[itemsetStr])
        else:
            self.get_tlist(itemset, itemsetStr)
            return len(self.vdb[itemsetStr])

    def getVDB(self, vdbPath):
        with open(vdbPath, 'r') as f:
            self.vdb = json.load(f)

    def saveVDB(self, vdbPath):
        temp = {}
        for k in self.vdb.keys():
            if "," not in k:
                temp[k] = list(self.vdb[k])
        with open(vdbPath, 'w') as f:
            json.dump(temp, f)

    def cleanup(self):
        self.vdb = {}
        self.vIncDB = {}
        self.mfis = []
        self.flist = []

    def updateStates(self, vIncDB, incDBSize):
        self.vIncDB = vIncDB
        self.dbSize = self.dbSize + incDBSize
        self.minsup = self.min_sup * self.dbSize
        for k, v in self.vIncDB.items():
            self.vdb[k] = self.vdb.get(k,set()).union(v)
        self.getFlist()


    def countItemsetVerticalInc(self, itemset, itemsetStr, p, newItemsetStr):
        if p in self.vIncDB:
            self.get_tlistInc(itemset, itemsetStr)
            new_tlist = self.vIncDB[itemsetStr].intersection(self.vIncDB[p])

            self.vIncDB[newItemsetStr] = new_tlist
            return len(new_tlist)

        return 0

    def fiCombineOrdInc(self, itemset, possibleSet):
        sortedItemset = sorted(itemset)
        itemsetStr = ",".join(sortedItemset)

        combineSetDict = {}
        for p in possibleSet:
            newItemsetStr =  ",".join(sorted(itemset + [p]))
            if newItemsetStr in self.retained:
                count = self.retained[newItemsetStr] + self.countItemsetVerticalInc(sortedItemset, itemsetStr, p, newItemsetStr)
            else:
                count = self.countItemsetVertical(sortedItemset, itemsetStr, p)

            if count >= self.minsup:
                combineSetDict[p] = count / (self.support(itemsetStr) * len(self.vdb[p]))
        combineSet = ascOrderedList(combineSetDict)
        return combineSet

    def runInc(self):
        self.backTrackInc([], self.flist, self.mfis)
        self.sortMFIs()

    def backTrackInc(self, itemset, combineSet, mfis):
        for c in combineSet:
            newItemset = itemset + [c]
            newPossibleSet = combineSet[combineSet.index(c)+1 : ]
            next = False
            p = -1
            for mfi in mfis:
                new_p = self.isNotSubSetPosition(newItemset + newPossibleSet, mfi)
                if new_p == -1:
                    next = True
                elif new_p > p:
                    p = new_p
            if next:
                return
            new_mfis = []
            newCombineSet = self.fiCombineOrdInc(newItemset, newPossibleSet)
            if not newCombineSet:
                if len(newItemset) >= p:
                    mfis.append(newItemset)
            else:
                new_mfis = [mfi for mfi in mfis if c in mfi]
                self.backTrackInc(newItemset, newCombineSet, new_mfis)
            for mfi in new_mfis:
                if mfi not in mfis:
                    mfis.append(mfi)

    ##### OPTIMIZE
    def updateRetainedFIs(self):
        for fi in self.generateFIs(self.mfis):
            if fi in self.retained:
                self.retained[fi] = self.retained[fi] + self.support(fi, True)
            else:
                self.retained[fi] = self.support(fi)

    def all_powersets(self, all_mfis):
        combinations = {}
        for mfi in all_mfis:
            prev_level = [[]]
            this_level = []
            # len(mfi) levels
            for i in range(len(mfi)):
                # add every item to pre_level
                for item in prev_level:
                    if item:
                        for j in range(mfi.index(item[-1]) + 1, len(mfi)):
                            this_level.append(item + [mfi[j]])
                    else:
                        this_level = [[item] for item in mfi]
                prev_level = this_level
                for comb in this_level:
                    combStr = ",".join(comb)
                    if combStr not in combinations:
                        combinations[combStr] = self.support(combStr)
                this_level = []
        self.cleanup()
        return dictToList(combinations)
