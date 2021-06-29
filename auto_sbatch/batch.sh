#!/bin/bash


if [ ! -d slurms/slurms$1 ]; then
	echo "Usage: arg1 [0-6]"
	exit 8
fi

i=0
#echo slurms$1
#for f in slurms/slurms$1/*;
for f in test/*;
do
	echo $f
	sbatch $f
	i=$((i+1))
	
	# snooze after submitting 6 batches of exps
	if [[ $(($i % 6)) -eq 0 ]]; then
		echo $i "snooze 20s"
		sleep 20s
	else	
		echo $i
		sleep 3s
	fi
done

echo "$i batches of exps DONE!"
