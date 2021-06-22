import argparse, math
from utils import *

parser = argparse.ArgumentParser(description='argparse')
parser.add_argument('--dbdir', '-p', help='database directory', required=True)
parser.add_argument('--database', '-d', help='database name', required=True)
parser.add_argument('--base', '-b', type=int, help='base x%', required=True)
parser.add_argument('--inc_number', '-i', type=int, help='number of increments', required=True)
args = parser.parse_args()

base = args.base/100
inc_number = args.inc_number

db = get_DB(args.dbdir, args.database)
print(len(db))

inc_split = math.floor(len(db) * base)
granularity = math.ceil((len(db) - inc_split)/inc_number)

output_dir = f"./base_{args.database}_{args.base}_{inc_number}"

baseDB = db[:inc_split]
writeDB(output_dir, baseDB, 0)

for i in range(inc_number):
    incDB = db[inc_split + granularity * i : min(len(db), inc_split + granularity * (i+1))]
    writeDB(output_dir, incDB, i + 1)
