# !/bin/bash
help() {
cat << EOF
Usage:
    -d default="test" ... database
    -m default="40" ... minsup
    -p default="2" ... partition
EOF
}

while getopts d:m:p: flag
do
    case "${flag}" in
        d) DB=${OPTARG};;
		    m) MINSUP=${OPTARG};;
        p) PARTITION=${OPTARG};;
        ?) help() ;;
    esac
done

# request type list is set to "post-get" by default
if [ -z $DB ]; then
    DB="test"
fi
# request number is set to 10 by default
if [ -z $MINSUP ]; then
    MINSUP="40"
fi
# thread number is set to 3 by default
if [ -z $PARTITION ]; then
    PARTITION="2"
fi

BASEDIR="/root/PFP"

DATADIR="$BASEDIR/data"
if [ ! -d $DATADIR ]; then
	mkdir $DATADIR
fi

MINSUPDIR="$DATADIR/$MINSUP"
if [ ! -d $MINSUPDIR ]; then
	mkdir $MINSUPDIR
fi

RESULTDIR="$MINSUPDIR/$PARTITION"
if [ ! -d $RESULTDIR ]; then
	mkdir $RESULTDIR
fi

python $BASEDIR/run.py -d $DB -m $MINSUP -p $PARTITION
