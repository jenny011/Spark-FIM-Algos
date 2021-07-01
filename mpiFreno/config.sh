#!/usr/bin/bash

HOME="/gpfsnyu/home/jz2915"

experiment="Freno"
expnums=7
#target="result"
target="perf"
#partition=4

#dataset="retail"
#dataset="kosarak"
#dataset="chainstore"
dataset="record"
ALLDATADIR="$HOME/incdatasets"
#datadir="$ALLDATADIR/$dataset"

DISTHOME="$HOME/distributed"
DIR="$DISTHOME/$experiment"

EXPDIR="$DIR/exp"
#path-to-performance/retail
performance="$EXPDIR/performance/$dataset"
memory="$EXPDIR/memory/$dataset"
result="$EXPDIR/result/$dataset"
tablesize="$EXPDIR/tablesize/$dataset"
#path-to-performance/retail/perf
perf="$performance"
mem="$memory/mem"
re="$result/result"
table="$tablesize/size"

run="$DIR/run.py"
runresult="$DIR/runresult.py"
runperf="$DIR/runperf.py"
runmem="$DIR/runmem.py"
runsize="$DIR/runsize.py"
