method = "Freno"
dataset = "kosarak"
partition = 16

#read nohup
res = {11:[0,0,0,0,0,0,0,0,0,0,0,0,0],21:[0,0,0,0,0,0,0,0,0,0,0,0,0],31:[0,0,0,0,0,0,0,0,0,0,0,0,0],41:[0,0,0,0,0,0,0,0,0,0,0,0,0],51:[0,0,0,0,0,0,0,0,0,0,0,0,0]}
f = open("nohup.out", "r")
line = f.readline()


flag = 0
sup = 11
remain = 0
while line:
    #print(line[37:39])
    if line[37:40] == "Job" and line[-2] == "s":
        print(line)
        #if flag:
        #    print(line)
        #    pos = line.find("took")
        #    res[sup].append(line[pos+5:len(line)-2])
        #    remain -= 1

        if not flag:
            pos = line.find("took")
            res[sup][remain] += float(line[pos+5:len(line)-2])
        else:
            pos = line.find("took")
            res[sup][remain] += float(line[pos+5:len(line)-2])
            remain += 1


        
        flag = abs(1-flag)
    line = f.readline()
    if remain == 13:
        remain = 0
        if sup != 51:
            sup += 10
        else:
            sup = 11
f.close()

print(res)
        

#save times
sv = []
for v in res.values():
    sv.append(",".join([str(k) for k in v])+"\n")

f = open("output_{0}_{1}_{2}.txt".format(method, dataset, partition), "w")
f.writelines(sv)
f.close()

