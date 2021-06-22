# !/bin/bash
help() {
cat << EOF
Usage:
    -p ... dbdir
    -d ... database
    -g ... granularity
EOF
}

while getopts p:d:g: flag
do
    case "${flag}" in
        p) DBDIR=${OPTARG};;
        d) DB=${OPTARG};;
        g) GRAN=${OPTARG};;
        ?) help ;;
    esac
done

if [ -z $DBDIR ]; then
  help
  exit -1
fi

if [ -z $DB ]; then
  help
  exit -1
fi

if [ -z $GRAN ]; then
  help
  exit -1
fi

OUTPUTDIR="./incdatasets/interval_$DB"_"$GRAN"
if [ ! -d $OUTPUTDIR ]; then
  echo $OUTPUTDIR
	mkdir $OUTPUTDIR
fi

python split_interval.py -p $DBDIR -d $DB -g $GRAN
