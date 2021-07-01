import numpy as np
import matplotlib.pyplot as plt
import os
from utils_chunk import *
from utils_scale import *
from utils_minsup import *

result_dir = "./exp_data"

def calcData(data):
	ret = {}
	sorted_keys = sorted(list(data.keys()))
	means = {}
	error = {}
	for k in sorted_keys:
		means[k] = np.mean(data[k])
		error[k] = np.std(data[k])
	return list(means.values()), list(error.values())


def calcDataChunk(data):
	ret = {}
	sorted_keys = list(data.keys())
	means = {}
	error = {}
	for k in sorted_keys:
		means[k] = np.mean(data[k])
		error[k] = np.std(data[k])
	print(sorted_keys, list(means.values()))
	return list(means.values()), list(error.values())






if __name__ == '__main__':
	algos = ["PFP",]
	datasets = ["retail", "kosarak", "chainstore"]
	dbSizes = {"retail":88162, "kosarak":990002, "chainstore":1112949, "record":574913}
	worker_num = [1,4,8,12]
	minsups = [1,11,21,31,41,51]
	chunks = [20000,40000,60000,80000,100000,0]

	
	for algo in algos:
		for dataset in datasets:
			
			for chunk in chunks:
				if chunk == 0:
					chunkStr = "static"
				else:
					chunkStr = str(chunk)[:-3] + "k"

				# minsup
				data1=[]
				for n in worker_num:
					means, errs = dataForPlotMinsup(algo, n, dataset, chunk)
					data1.append((n, means, errs))

				plotSpeedMinsup(algo, dataset, minsups, data1, chunkStr)


				# scaling
				data2 = []
				for minsup in minsups:
					means, errs = dataForPlotScale(algo, minsup, dataset, chunk)
					data2.append((minsup, means, errs, algo))

				plotSpeedScale(algo, dataset, worker_num, data2, chunkStr)


			# chunk
			for n in worker_num:
				data3=[]
				for minsup in minsups:
					means, errs = dataForPlotChunk(algo, minsup, dataset, n)
					data3.append((minsup, means, errs))

				this_chunks = ["20k", "40k", "60k", "80k", "100k", str(dbSizes[dataset])]

				plotSpeedChunk(algo, dataset, this_chunks, data3, n)


	# ======================================================================
	# ======================================================================
	# ======================================================================

	# compare algos
	# for dataset in datasets:
	# 	# scaling
	# 	for chunk in chunks:
	# 		if chunk == 0:
	# 			chunkStr = "static"
	# 		else:
	# 			chunkStr = str(chunk)[:-3] + "k"

	# 		for minsup in minsups:
	# 			data1 = []
	# 			for algo in algos:
	# 				means, errs = dataForPlotScale(algo, minsup, dataset, chunk)
	# 				data1.append((minsup, means, errs, algo))

	# 			plotSpeedScale("compare", dataset, worker_num, data1, chunkStr)
							
