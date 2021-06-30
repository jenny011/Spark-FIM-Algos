#!/bin/bash
SPARK_HOME='/usr/local/hadoop/spark-2.1.0-bin-hadoop2.7'
MASTERIP=192.168.1.244

OUTPUTDIR="/root/exp_mem"
if [ ! -d $OUTPUTDIR ]; then
	mkdir $OUTPUTDIR
fi

for ALGO in "PFP"
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
		for MINSUP in 51
		do
			for INTERVAL in 80000
			do
				for PARTITION in 2
				do
					EXPDIR="$DBDIR/$MINSUP"_"$INTERVAL"_"$PARTITION"
					if [ ! -d $EXPDIR ]; then
						mkdir $EXPDIR
					fi
					for NUM in 0
					do
						# run exp
						OUTFILE="$EXPDIR/$NUM.out"
						APPNAME="$ALGO-$DB-$MINSUP-$INTERVAL-$PARTITION-$NUM"

						nohup /usr/local/hadoop/spark-2.1.0-bin-hadoop2.7/bin/spark-submit \
						--py-files Archive.zip \
						--name $APPNAME\
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
						--conf spark.metrics.namespace=$APPNAME\
						--conf spark.metrics.conf=$SPARK/conf/metrics.properties\
						run.py -d $DB -m $MINSUP -p $PARTITION -i $INTERVAL > $OUTFILE

						# process exp output
						RESULTDIR="/root/exp_result"
						#if [ ! -d $RESULTDIR ]; then
						#	mkdir $RESULTDIR
						#fi
						#if [ ! -d "$RESULTDIR/$ALGO" ]; then
						#	mkdir "$RESULTDIR/$ALGO"
						#fi
						#if [ ! -d "$RESULTDIR/$ALGO/$DB" ]; then
						#	mkdir "$RESULTDIR/$ALGO/$DB"
						#fi
						#if [ ! -d "$RESULTDIR/$ALGO/$DB/$MINSUP"_"$INTERVAL"_"$PARTITION" ]; then
						#	mkdir "$RESULTDIR/$ALGO/$DB/$MINSUP"_"$INTERVAL"_"$PARTITION"
						#fi

						#python3 /root/process_output.py -a $ALGO -d $DB -m $MINSUP -p $PARTITION -i $INTERVAL -n $NUM
					done
				done
			done
		done
	done
done
