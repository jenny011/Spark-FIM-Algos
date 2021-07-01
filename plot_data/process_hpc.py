rawdatadir = "./exp_output"
resultdir = "./exp_data"

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
        for partition in [1]:
            raw_data_path = os.path.join(rawdatadir, algo, db, f"{partition}/{support}_{interval}_{partition}.txt")
            
            res = []
            fis = ""
            with open(raw_data_path, "r") as f:
                fis = f.readline().rstrip()
                for line in f:
                    if line[:4] == 'real':
                        res.append(float(line.rstrip().replace("real ", "")))

            # with open(os.path.join(rawdatadir, algo, db, "result", f"{support}_{interval}_{partition}.json"), "w") as fif:
            #     fis = fiStr.lstrip("['").rstrip("']\n").split("', '")
            #     print(fis)
                # json.dump(fis, fif) 
            fis = fis.lstrip("['").rstrip("']\n").split("', '")
            print(fis)                      

            #save times
            times = []
            if len(res) != expnum:
                print(len(res), raw_data_path)
                print("Expnum Error")
                sys.exit(-1)
            for v in res:
                times.append(str(v)+'\n')
            print(times)

            # with open(os.path.join(resultdir, algo, db, f"{support}_{interval}_{partition}.txt"), "a") as f:
            #     f.writelines(times)


