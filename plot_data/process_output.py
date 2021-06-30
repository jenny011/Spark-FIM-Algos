outputdir = "./exp_output"
resultdir = "./exp_data"
#fidir = "./exp_fis"

import os, json, argparse, sys

parser = argparse.ArgumentParser(description='argparse')
parser.add_argument('--algo', '-a', help='algo', required=True)
parser.add_argument('--expnum', '-n', type=int, help='expnum', required=True)
parser.add_argument('--db', '-d', help='db', required=True)
args = parser.parse_args()

algo = args.algo
expnum = args.expnum
db = args.db

for support in range(1, 61, 10):
    for interval in range(0,120000,20000):
        for partition in [4,8,12]:
            res = [0 for i in range(expnum)]

            for i in range(expnum):
                output_path = os.path.join(outputdir, algo, db, "perf", f"{support}_{interval}_{partition}", f"{i}.txt")

                # if i == 0:
                #     fiStr = ''

                with open(output_path, "r") as f:
                    # prev = ''
                    line = f.readline()

                    while line:
                        if line[37:40] == "Job" and line[-2] == "s":
                            pos = line.find("took")
                            res[i] += float(line[pos+5:len(line)-2])

                        # if i == 0:
                        #     if line[18:52] == "INFO SparkUI: Stopped Spark web UI":
                        #         fiStr = prev
                        #     prev = line

                        line = f.readline() 

                # if i == 0:
                #     with open(os.path.join(fidir, algo, db, f"{support}_{interval}_{partition}.json"), "w") as fif:
                #         fis = fiStr.lstrip("{'").rstrip("'}\n").split("', '")
                #         json.dump(fis, fif)                       

            #save times
            times = []
            if len(res) != expnum:
                print("Expnum Error")
                sys.exit(-1)
            for v in res:
                times.append(str(v)+'\n')

            with open(os.path.join(resultdir, algo, db, f"{support}_{interval}_{partition}.txt"), "a") as f:
                f.writelines(times)


