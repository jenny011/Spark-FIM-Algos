#!/bin/bash
MASTERIP=192.168.1.244

OUTPUTDIR="/root/exp_output"
if [ ! -d $OUTPUTDIR ]; then
	mkdir $OUTPUTDIR
fi

for ALGO in "IncMiningPFP"
do
	ALGODIR="$OUTPUTDIR/$ALGO"
	if [ ! -d $ALGODIR ]; then
		mkdir $ALGODIR
	fi
	for DB in "kosarak"
	do
		DBDIR="$ALGODIR/$DB"
		if [ ! -d $DBDIR ]; then
			mkdir $DBDIR
		fi
		for MINSUP in 31 21 11 1
		do
			for INTERVAL in 80000 60000 40000 20000 0
			do
				for PARTITION in 4
				do
					EXPDIR="$DBDIR/$MINSUP"_"$INTERVAL"_"$PARTITION"
					if [ ! -d $EXPDIR ]; then
						mkdir $EXPDIR
					fi
					for NUM in 0
					do
						# run exp
						OUTFILE="$EXPDIR/$NUM.out"
						for group in $( seq 0 1 $(( $PARTITION-1 )) )
						do 
							rm -f "./data/result"_"$group.json"
							touch "./data/result"_"$group.json"
						done

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
				done
			done
		done
	done
done
