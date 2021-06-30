import numpy as np
import matplotlib.pyplot as plt
import os

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

def plotSpeedMinsup(algo, dataset, x, listOfMeanErr, chunk):
	fig = plt.figure()
	ax = fig.add_subplot(111)
	ax.set_xlabel('minsup(%)', fontsize = 12)
	ax.set_ylabel("running time (second)", fontsize = 12)

	for item in listOfMeanErr:
		ax.errorbar(x, item[1], yerr=item[2], linestyle="dashed", marker="", markersize=10, linewidth=1, elinewidth=1.5, capsize=3, label=f"{item[0]} workers")

	ax.set_ylim(bottom=0)
	ax.legend(loc='best')
	if chunk == "static":
		plt.title(f'{algo} {dataset} minsup - {chunk}')
	else:
		plt.title(f'{algo} {dataset} minsup - chunk size {chunk}')
	plt.show()
	fig.savefig(f'graphs/{algo}/minsup/{algo}_{dataset}_minsup_{chunk}.png')





def processPerfDataScale(fdir, minsup, chunk):
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

def dataForPlotScale(algo, minsup, dataset, chunk):
	fdir = os.path.join(result_dir, algo, dataset)
	raw = processPerfDataScale(fdir, minsup, chunk)
	return calcData(raw)

def plotSpeedScale(algo, data, x, listOfMeanErr, chunk):
	fig = plt.figure()
	ax = fig.add_subplot(111)
	ax.set_xlabel('number of workers', fontsize = 12)
	ax.set_ylabel("running time (second)", fontsize = 12)

	for item in listOfMeanErr:
		ax.errorbar(x, item[1], yerr=item[2], linestyle="dashed", marker="", markersize=10, linewidth=1, elinewidth=1.5, capsize=3, label=f"{item[0]}%")

	ax.set_ylim(bottom=0)
	ax.legend(loc='best')
	if chunk == "static":
		plt.title(f'{algo} {dataset} scale - {chunk}')
	else:
		plt.title(f'{algo} {dataset} scale - chunk size {chunk}')
	plt.show()
	fig.savefig(f'graphs/{algo}/scale/{algo}_{dataset}_scale_{chunk}.png')



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

def plotSpeedChunk(algo, dataset, x, listOfMeanErr, workerNum):
	fig = plt.figure()
	ax = fig.add_subplot(111)
	ax.set_xlabel('chunk size', fontsize = 12)
	ax.set_ylabel("running time (second)", fontsize = 12)

	for item in listOfMeanErr:
		ax.errorbar(x, item[1], yerr=item[2], linestyle="dashed", marker="", markersize=10, linewidth=1, elinewidth=1.5, capsize=3, label=f"{item[0]}%")

	ax.set_ylim(bottom=0)
	ax.legend(loc='best')
	plt.title(f'{algo} {dataset} increment - {workerNum} worker(s)')
	plt.show()
	fig.savefig(f'graphs/{algo}/inc/{algo}_{dataset}_increment_{workerNum}.png')




if __name__ == '__main__':
	algos = ["PFP",]
	datasets = ["record"]
	dbSizes = {"retail":88162, "kosarak":990002, "chainstore":1112949, "record":574913}
	worker_num = [4,8,12]
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
							
