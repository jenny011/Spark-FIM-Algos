method = "distFreno"
dataset = "retail"
partition = 16

#read nohup
res = {1:[],6:[],11:[],16:[],21:[],26:[],31:[],36:[],41:[],46:[]}
f = open("nohup.out", "r")
line = f.readline()


flag = 0
sup = 1
remain = 13
while line:
    #print(line[37:39])
    if line[37:40] == "Job":
        if flag:
            print(line)
            pos = line.find("took")
            res[sup].append(line[pos+5:len(line)-2])
            remain -= 1
        
        flag = abs(1-flag)
    line = f.readline()
    if remain == 0:
        remain = 13
        sup += 5
f.close()

print(res)
        

#save times
sv = []
for v in res.values():
    sv.append(",".join(v)+"\n")

f = open("output_{0}_{1}_{2}.txt".format(method, dataset, partition), "w")
f.writelines(sv)
f.close()

