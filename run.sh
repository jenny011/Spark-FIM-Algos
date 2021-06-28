#!/bin/bash
# put it in $ALGO directory
MASTERIP=192.168.1.244

OUTPUTDIR="/root/exp_output"
if [ ! -d $OUTPUTDIR ]; then
	mkdir $OUTPUTDIR
fi

for ALGO in "PFP"
do
	ALGODIR="$OUTPUTDIR/$ALGO"
	if [ ! -d $ALGODIR ]; then
		mkdir $ALGODIR
	fi
	for DB in "retail"
	do
		DBDIR="$ALGODIR/$DB"
		if [ ! -d $DBDIR ]; then
			mkdir $DBDIR
		fi
		for MINSUP in 1 11 21 31 41 51
		do
			for INTERVAL in 0 20000 40000 60000 80000 100000
			do
				PARTITION=1
				# for PARTITION in 1 4 8 16
				# do
					EXPDIR="$DBDIR/$MINSUP"_"$INTERVAL"_"$PARTITION"
					if [ ! -d $EXPDIR ]; then
						mkdir $EXPDIR
					fi
					for NUM in 0 1 2 3 4 5 6 7 8 9 10 11 12
					do
						# run exp
						OUTFILE="$EXPDIR/$NUM.out"

						nohup /usr/local/hadoop/spark-2.1.0-bin-hadoop2.7/bin/spark-submit \
						--py-files Archive.zip \
						--master spark://master.hadoop:7077 \
						--conf spark.executorEnv.PYTHONHASHSEED=321 \
						--driver-cores 1 \
						--driver-memory 61g \
						--num-executors $PARTITION \
						--executor-cores 1 \
						--executor-memory 14g \
						--conf spark.rpc.message.maxSize=1024 \
						--conf spark.driver.maxResultSize=0 \
						--conf spark.default.parallelism=$PARTITION \
						run.py -d $DB -m $MINSUP -p $PARTITION -i $INTERVAL > $OUTFILE

						# process exp output
						RESULTDIR="/root/exp_result"
						if [ ! -d $RESULTDIR ]; then
							mkdir $RESULTDIR
						fi
						if [ ! -d "$RESULTDIR/$ALGO" ]; then
							mkdir "$RESULTDIR/$ALGO"
						fi
						if [ ! -d "$RESULTDIR/$ALGO/$DB" ]; then
							mkdir "$RESULTDIR/$ALGO/$DB"
						fi
						if [ ! -d "$RESULTDIR/$ALGO/$DB/$MINSUP"_"$INTERVAL"_"$PARTITION" ]; then
							mkdir "$RESULTDIR/$ALGO/$DB/$MINSUP"_"$INTERVAL"_"$PARTITION"
						fi

						python3 /root/process_output.py -a $ALGO -d $DB -m $MINSUP -p $PARTITION -i $INTERVAL -n $NUM
					done
				# done
			done
		done
	done
done
