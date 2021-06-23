import argparse, math
from utils import *

parser = argparse.ArgumentParser(description='argparse')
parser.add_argument('--dbdir', '-p', help='database directory', required=True)
parser.add_argument('--database', '-d', help='database name', required=True)
parser.add_argument('--granularity', '-g', type=int, help='size of increment', required=False, default=0)
args = parser.parse_args()


granularity = args.granularity

db = get_DB(args.dbdir, args.database)
print(len(db))

output_dir = f"../SpFreno/incdatasets/interval_{args.database}_{granularity}"

for i in range(math.ceil(len(db) / granularity)):
    incDB = db[granularity * i : min(granularity * (i+1), len(db))]
    writeDB(output_dir, incDB, i)
