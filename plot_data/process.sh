#!/bin/bash

for ALGO in PFP
do
	if [ ! -d ./graphs ]; then
		mkdir ./graphs
	fi

	if [ ! -d ./graphs/$ALGO ]; then
		mkdir ./graphs/$ALGO
	fi

	for item in minsup scale inc
	do
		if [ ! -d ./graphs/$ALGO/$item ]; then
			mkdir ./graphs/$ALGO/$item
		fi
	done

	for DB in retail kosarak chainstore
	do
		if [ ! -d ./exp_data/$ALGO/$DB ]; then
			mkdir ./exp_data/$ALGO/$DB
		fi
		# if [ ! -d ./exp_fis/$ALGO/$DB ]; then
		# 	mkdir ./exp_fis/$ALGO/$DB
		# fi
		# rm -r ./exp_result/$ALGO/$DB/*

		python process_output.py -a $ALGO -d $DB -n 7
	done
done

