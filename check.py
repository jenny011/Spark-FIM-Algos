import json

with open("./check.json", 'r') as f:
	check = json.load(f)

fi = check["z-r"]
pfp = check["pfp-r"]
fp = check["fp-r"]
print(len(fi), len(pfp), len(fp))


def checkpfp():
	for i in range(len(pfp)):
		pfp[i] = ",".join(sorted(pfp[i].split(",")))

	for j in range(len(fp)):
		fp[j] = ",".join(sorted(fp[j].split(",")))


	ret1 = []
	for i in pfp:
		if i not in fp:
			ret1.append(i)
	print("111111", ret1)

	ret2 = []
	for i in fp:
		if i not in pfp:
			ret2.append(i)
	print("222222", ret2)

def checkfi():
	for i in range(len(fi)):
		fi[i] = ",".join(sorted(fi[i].split(",")))

	for j in range(len(fp)):
		fp[j] = ",".join(sorted(fp[j].split(",")))


	r1 = set(fi)
	r2 = set(fp)
	print(len(r1), len(r2))
	print(r1.difference(r2))
	print(r2.difference(r1))

	d = {}
	for i in fi:
		d[i] = d.get(i, 0) + 1
	for k,v in d.items():
		if v > 1:
			print(k, v)


def checkmax():
	mfi = [['10516'], ['265'], ['2959'], ['46'], ['243'], ['957'], ['32'], ['480'], ['3271'], ['784'], ['176'], ['523'], ['259'], ['180'], ['20'], ['162'], ['118'], ['13042'], ['79'], ['38', '39'], ['1005'], ['678'], ['590'], ['50'], ['202'], ['549'], ['15833'], ['250'], ['1394'], ['16218'], ['741'], ['287', '39'], ['302'], ['605'], ['825'], ['593'], ['339'], ['14099'], ['124'], ['16011'], ['10'], ['186'], ['1147', '40'], ['12926', '40'], ['256', '49'], ['256', '40'], ['534', '40'], ['61', '40'], ['80', '49'], ['80', '40'], ['2239', '49'], ['2239', '40'], ['271', '49'], ['271', '40'], ['148', '49'], ['148', '40'], ['1328', '49'], ['1328', '40'], ['439', '49'], ['439', '40'], ['414', '40'], ['414', '49'], ['272', '49'], ['272', '40'], ['476', '49', '40'], ['102', '49', '40'], ['311', '49', '40'], ['111', '49', '40', '39'], ['37', '49', '40', '39'], ['238', '49', '40'], ['171', '49', '40', '39'], ['226', '49', '40'], ['90', '40', '49'], ['66', '42'], ['66', '49', '40'], ['42', '33', '49', '40'], ['42', '39', '49', '40'], ['33', '39', '49', '40']]
	lmfi = [['10516'], ['265'], ['2959'], ['46'], ['243'], ['957'], ['32'], ['480'], ['3271'], ['784'], ['176'], ['523'], ['259'], ['180'], ['20'], ['162'], ['118'], ['13042'], ['79'], ['38', '39'], ['1005'], ['678'], ['590'], ['50'], ['202'], ['549'], ['15833'], ['250'], ['1394'], ['16218'], ['741'], ['287', '39'], ['302'], ['605'], ['825'], ['593'], ['339'], ['14099'], ['124'], ['16011'], ['10'], ['186'], ['1147', '40'], ['12926', '40'], ['256', '49'], ['256', '40'], ['534', '40'], ['61', '40'], ['80', '49'], ['80', '40'], ['2239', '49'], ['2239', '40'], ['271', '49'], ['271', '40'], ['148', '49'], ['148', '40'], ['1328', '49'], ['1328', '40'], ['439', '49'], ['439', '40'], ['414', '40'], ['414', '49'], ['272', '49'], ['272', '40'], ['476', '49', '40'], ['102', '49', '40'], ['311', '49', '40'], ['111', '49', '40', '39'], ['37', '49', '40', '39'], ['238', '49', '40'], ['171', '49', '40', '39'], ['226', '49', '40'], ['90', '40', '49'], ['66', '42'], ['66', '49', '40'], ['42', '33', '49', '40'], ['42', '39', '49', '40'], ['33', '39', '49', '40']]
	print(mfi == lmfi)

checkfi()

# with open("list.json", 'w') as f:
# 	print(type({"1":1}))
# 	json.dump({"1":1}, f)

# with open("vdb.json", 'r') as f:
# 	check = json.load(f)
# 	print(check["81"])