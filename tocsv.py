import numpy as np
import matplotlib.pyplot as plt
import os, csv

result_dir = "./exp_result"

def calcData(data):
	ret = {}
	sorted_keys = sorted(list(data.keys()))
	means = {}
	error = {}
	for k in sorted_keys:
		means[k] = "{:.2f}".format(round(np.mean(data[k]), 2))
		error[k] = "{:.2f}".format(round(np.std(data[k]), 2))
	return list(means.values()), list(error.values())


def processPerfDataMinsup(fdir, workerNum, chunk):
	ret = {}
	for minsup in minsups:
		fname = f"{minsup}_{chunk}_{workerNum}.txt"
		fpath = os.path.join(fdir, fname)
		ret[minsup] = []
		with open(fpath, 'r') as file:
			for line in file:
				if line:
					ret[minsup].append(float(line.rstrip()))
	for k in ret.keys():
		maximum = max(ret[k])
		minimum = min(ret[k])
		ret[k].remove(maximum)
		ret[k].remove(minimum)
	return ret

def dataForPlotMinsup(algo, workerNum, dataset, chunk):
	fdir = os.path.join(result_dir, algo, dataset)
	raw = processPerfDataMinsup(fdir, str(workerNum), chunk)
	return calcData(raw)



def processPerfDataWorker(fdir, minsup, chunk):
	ret = {}
	for n in worker_num:
		fname = f"{minsup}_{chunk}_{n}.txt"
		fpath = os.path.join(fdir, fname)
		ret[n] = []
		with open(fpath, 'r') as file:
			for line in file:
				if line:
					ret[n].append(float(line.rstrip()))
	for k in ret.keys():
		maximum = max(ret[k])
		minimum = min(ret[k])
		ret[k].remove(maximum)
		ret[k].remove(minimum)
	return ret

def dataForPlotWorker(algo, minsup, dataset, chunk):
	fdir = os.path.join(result_dir, algo, dataset)
	raw = processPerfDataWorker(fdir, minsup, chunk)
	return calcData(raw)



def calcDataChunk(data):
	ret = {}
	sorted_keys = list(data.keys())
	means = {}
	error = {}
	for k in sorted_keys:
		means[k] = "{:.2f}".format(round(np.mean(data[k]), 2))
		error[k] = "{:.2f}".format(round(np.std(data[k]), 2))
	return list(means.values()), list(error.values())


def processPerfDataChunk(fdir, minsup, workerNum):
	ret = {}
	for chunk in chunks:
		fname = f"{minsup}_{chunk}_{workerNum}.txt"
		fpath = os.path.join(fdir, fname)
		ret[chunk] = []
		with open(fpath, 'r') as file:
			for line in file:
				if line:
					ret[chunk].append(float(line.rstrip()))
	for k in ret.keys():
		maximum = max(ret[k])
		minimum = min(ret[k])
		ret[k].remove(maximum)
		ret[k].remove(minimum)
	return ret

def dataForPlotChunk(algo, minsup, dataset, workerNum):
	fdir = os.path.join(result_dir, algo, dataset)
	raw = processPerfDataChunk(fdir, minsup, workerNum)
	return calcDataChunk(raw)




if __name__ == '__main__':
	algos = ["PFP"]
	datasets = ["retail", "kosarak", "chainstore"]
	dbSizes = {"retail":88162, "kosarak":990002, "chainstore":1112949, "record":574913}
	worker_num = [16]
	minsups = [1,11,21,31,41,51]
	chunks = [20000,40000,60000,80000,100000,0]

	
	for algo in algos:
		#column
		for dataset in datasets:
			
			for chunk in chunks:
				if chunk == 0:
					chunkStr = "static"
				else:
					chunkStr = str(chunk)[:-3] + "k"

				#row
				# minsup
				data1=[]
				for n in worker_num:
					means, errs = dataForPlotMinsup(algo, n, dataset, chunk)
					data1.append([n, means])

				csvlines = [[m] for m in minsups]
				for item in data1:
					for i in range(len(item[1])):
						csvlines[i].append(item[1][i])
				header = ["minsup"]
				for w in worker_num:
					header.append(w)


				with open(f"minsup/minsup_{algo}_{dataset}_{chunk}.csv", "w") as f:
					writer = csv.writer(f, delimiter=',')
					writer.writerow(header)
					for item in csvlines:
						writer.writerow(item)


				# scaling
				data2=[]
				for minsup in minsups:
					means, errs = dataForPlotWorker(algo, minsup, dataset, chunk)
					data2.append([minsup, means])

				csvlines = [[w] for w in worker_num]
				for item in data2:
					for i in range(len(item[1])):
						csvlines[i].append(item[1][i])
				header = ["worker_num"]
				for m in minsups:
					header.append(m)

				with open(f"scale/scale_{algo}_{dataset}_{chunk}.csv", "w") as f:
					writer = csv.writer(f, delimiter=',')
					writer.writerow(header)
					for item in csvlines:
						writer.writerow(item)


			# chunk
			for n in worker_num:
				data3=[]
				for minsup in minsups:
					means, errs = dataForPlotChunk(algo, minsup, dataset, n)
					data3.append([minsup, means])

				csvlines = [["20k"], ["40k"], ["60k"], ["80k"], ["100k"], [str(dbSizes[dataset])]]
				for item in data3:
					for i in range(len(item[1])):
						csvlines[i].append(item[1][i])
				header = ["minsup"]
				for m in minsups:
					header.append(m)

				with open(f"inc/inc_{algo}_{dataset}_{n}.csv", "w") as f:
					writer = csv.writer(f, delimiter=',')
					writer.writerow(header)
					for item in csvlines:
						writer.writerow(item)


	# ======================================================================
	# ======================================================================
	# ======================================================================

	# for dataset in datasets:
	# 	# scaling
		
	# 	for minsup in minsups:
	# 		data1 = []
	# 		for algo in algos:
	# 			means, errs = dataForPlotWorker(algo, minsup, dataset, 20000)
	# 			data1.append((minsup, means, errs, algo))

	# 		plotSpeedWorker("pfp vs freno", dataset, worker_num, data1, "20k")
						
