# !/bin/bash
help() {
cat << EOF
Usage:
    -p ... dbdir
    -d ... database
    -b ... base x%
    -i ... inc_number
EOF
}

while getopts p:d:b:i: flag
do
    case "${flag}" in
        p) DBDIR=${OPTARG};;
        d) DB=${OPTARG};;
        b) BASE=${OPTARG};;
		    i) INC=${OPTARG};;
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

if [ -z $BASE ]; then
  help
  exit -1
fi

if [ -z $INC ]; then
  help
  exit -1
fi


OUTPUTDIR="./base_$DB"_"$BASE"_"$INC"
if [ ! -d $OUTPUTDIR ]; then
  echo $OUTPUTDIR
	mkdir $OUTPUTDIR
fi

python split_base.py -p $DBDIR -d $DB -b $BASE -i $INC
