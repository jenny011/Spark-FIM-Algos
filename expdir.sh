# !/bin/bash

# for algo in "PFP" "IncMiningPFP" "distZigZag"
# do
# 	for database in "retail" "kosarak" "chainstore" "record"
# 	do
# 		for minsup in 1 11 21 31 41 51
# 		do
# 			for interval in 0 20000 40000 60000 80000 100000
# 			do
# 				DIR="./$algo/data/$database"_"$minsup"_"4"_"$interval"
# 				echo $DIR
# 				if [ ! -d  $DIR ]; then
# 					mkdir $DIR
# 				fi
# 			done
# 		done
# 	done
# done

PARTITION=4

for group in $( seq 0 1 $(( $PARTITION-1 )) )
do 
	touch "./result"_"$group.json"
done
