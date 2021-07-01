#!/bin/bash

for ALGO in PFP
do
	for DB in retail
	do
		if [ ! -d ./exp_data/$ALGO/$DB ]; then
			mkdir ./exp_data/$ALGO/$DB
		fi
		# if [ ! -d ./exp_fis/$ALGO/$DB ]; then
		# 	mkdir ./exp_fis/$ALGO/$DB
		# fi
		# rm -r ./exp_result/$ALGO/$DB/*

		python process_spark.py -a $ALGO -d $DB -n 7
		python process_hpc.py -a $ALGO -d $DB -n 7
	done
done

