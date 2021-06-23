# !/bin/bash


BASEDIR="."

DATADIR="$BASEDIR/data"
if [ ! -d $DATADIR ]; then
	mkdir $DATADIR
fi

for MINSUP in 1 6 11 16 21 26 31 36 41 46
do

    MINSUPDIR="$DATADIR/$MINSUP"
    if [ ! -d $MINSUPDIR ]; then
        mkdir $MINSUPDIR
    fi
    
    for PARTITION in 1 2 4 8 16 32
    do
    
        RESULTDIR="$MINSUPDIR/$PARTITION"
        if [ ! -d $RESULTDIR ]; then
            mkdir $RESULTDIR
        fi
        
    done
    
done

python3 $BASEDIR/run.py
