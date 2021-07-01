import numpy as np
import matplotlib.pyplot as plt
import os

basedir = "./performance"


def calcData(data):
	ret = {}
	sorted_keys = sorted(list(data.keys()))
	means = {}
	error = {}
	for k in sorted_keys:
		means[k] = np.mean(data[k])
		error[k] = np.std(data[k])
	return list(means.values()), list(error.values())


def processPerfDataMinsup(fdir, chunk):
	ret = {}
	for minsup in minsups:
		fname = f"{minsup}_{chunk}.txt"
		fpath = os.path.join(fdir, fname)
		ret[minsup] = []
		with open(fpath, 'r') as file:
			for line in file:
				if line[:4] == 'real':
					ret[minsup].append(float(line.rstrip().replace("real ", "")))
	for k in ret.keys():
		maximum = max(ret[k])
		minimum = min(ret[k])
		ret[k].remove(maximum)
		ret[k].remove(minimum)
	return ret

def dataForPlotMinsup(workerNum, dataset, chunk):
	fdir = os.path.join(basedir, dataset, str(workerNum))
	raw = processPerfDataMinsup(fdir, chunk)
	return calcData(raw)

def plotSpeedMinsup(dataset, x, listOfMeanErr, chunk):
	fig = plt.figure()
	ax = fig.add_subplot(111)
	ax.set_xlabel('minsup(%)', fontsize = 12)
	ax.set_ylabel("running time (second)", fontsize = 12)


	for item in listOfMeanErr:
		ax.errorbar(x, item[1], yerr=item[2], linestyle="dashed", marker="", markersize=10, linewidth=1, elinewidth=1.5, capsize=3, label=f"{item[0]} workers")

	ax.set_ylim(bottom=0)
	ax.legend(loc='best')
	if chunk == "static":
		plt.title(f'{dataset} minsup - {chunk}')
	else:
		plt.title(f'{dataset} minsup - chunk size {chunk}')
	plt.show()
	#"distFreno_mpi_minsup/" + 
	fig.savefig(f'{dataset}_minsup_{chunk}.png')





def processPerfDataScale(fprefix, minsup, chunk):
	ret = {}
	for n in worker_num:
		fdir = os.path.join(fprefix, str(n))
		fname = f"{minsup}_{chunk}.txt"
		fpath = os.path.join(fdir, fname)
		ret[n] = []
		with open(fpath, 'r') as file:
			for line in file:
				if line[:4] == 'real':
					ret[n].append(float(line.rstrip().replace("real ", "")))
	for k in ret.keys():
		maximum = max(ret[k])
		minimum = min(ret[k])
		ret[k].remove(maximum)
		ret[k].remove(minimum)
	return ret

def dataForPlotScale(minsup, dataset, chunk):
	fprefix = os.path.join(basedir, dataset)
	raw = processPerfDataScale(fprefix, minsup, chunk)
	return calcData(raw)

def plotSpeedScale(dataset, x, listOfMeanErr, chunk):
	fig = plt.figure()
	ax = fig.add_subplot(111)
	ax.set_xlabel('number of workers', fontsize = 12)
	ax.set_ylabel("running time (second)", fontsize = 12)
	

	for item in listOfMeanErr:
		ax.errorbar(x, item[1], yerr=item[2], linestyle="dashed", marker="", markersize=10, linewidth=1, elinewidth=1.5, capsize=3, label=f"{item[0]}%")

	ax.set_ylim(bottom=0)
	ax.legend(loc='best')
	if chunk == "static":
		plt.title(f'{dataset} scale - {chunk}')
	else:
		plt.title(f'{dataset} scale - chunk size {chunk}')
	plt.show()
	#"distFreno_mpi_scaling/" + 
	fig.savefig(f'{dataset}_scale_{chunk}.png')




def calcDataChunk(data):
	ret = {}
	sorted_keys = list(data.keys())
	means = {}
	error = {}
	for k in sorted_keys:
		means[k] = np.mean(data[k])
		error[k] = np.std(data[k])
	return list(means.values()), list(error.values())


def processPerfDataChunk(fdir, minsup):
	ret = {}
	for chunk in chunks:
		fname = f"{minsup}_{chunk}.txt"
		fpath = os.path.join(fdir, fname)
		ret[chunk] = []
		with open(fpath, 'r') as file:
			for line in file:
				if line[:4] == 'real':
					ret[chunk].append(float(line.rstrip().replace("real ", "")))
	for k in ret.keys():
		maximum = max(ret[k])
		minimum = min(ret[k])
		ret[k].remove(maximum)
		ret[k].remove(minimum)
	return ret

def dataForPlotChunk(minsup, dataset, workerNum):
	fdir = os.path.join(basedir, dataset, str(workerNum))
	raw = processPerfDataChunk(fdir, minsup)
	return calcDataChunk(raw)

def plotSpeedChunk(dataset, x, listOfMeanErr, workerNum):
	fig = plt.figure()
	ax = fig.add_subplot(111)
	ax.set_xlabel('chunk size', fontsize = 12)
	ax.set_ylabel("running time (second)", fontsize = 12)

	for item in listOfMeanErr:
		ax.errorbar(x, item[1], yerr=item[2], linestyle="dashed", marker="", markersize=10, linewidth=1, elinewidth=1.5, capsize=3, label=f"{item[0]} %")

	ax.set_ylim(bottom=0)
	ax.legend(loc='best')
	plt.title(f'{dataset} increment - {workerNum} worker(s)')
	plt.show()
	#"distFreno_mpi_minsup/" + 
	fig.savefig(f'{dataset}_increment_{workerNum}.png')





if __name__ == '__main__':
	datasets = ["retail", "kosarak"]
	dbSizes = {"retail":88162, "kosarak":990002, "chainstore":1112949, "record":574913}
	worker_num = [1,4,6,8,10]
	minsups = [1,11,21,31,41,51]
	chunks = [20000,40000,60000,80000,100000,0]

	
	for dataset in datasets:
		
		for chunk in chunks:
			if chunk == 0:
				chunkStr = "static"
			else:
				chunkStr = str(chunk)[:-3] + "k"

			
			# minsup
			data1=[]
			for n in worker_num:
				freno, efreno = dataForPlotMinsup(n, dataset, chunk)
				data1.append((n, freno, efreno))

			plotSpeedMinsup(dataset, minsups, data1, chunkStr)

			
			# scaling
			data2=[]
			for minsup in minsups:
				freno, efreno = dataForPlotScale(minsup, dataset, chunk)
				data2.append((minsup, freno, efreno))

			plotSpeedScale(dataset, worker_num, data2, chunkStr)


		# chunk
		for n in worker_num:
			data3=[]
			for minsup in minsups:
				freno, efreno = dataForPlotChunk(minsup, dataset, n)
				data3.append((minsup, freno, efreno))

			this_chunks = ["20k", "40k", "60k", "80k", "100k", str(dbSizes[dataset])]

			plotSpeedChunk(dataset, this_chunks, data3, n)

			

