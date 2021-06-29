#!/bin/bash


if [ ! -d slurms/slurms$1 ]; then
	echo "Usage: arg1 [0-6]"
	exit 8
fi

i=0
echo slurms$1

for d in retail kosarak chainstore record;
do
	for ((c=0;c<=100000;c+=20000));
	do
		for f in slurms/slurms$1/$d/$c/*;
		#for f in test/*;
		do
			echo $f
			#sbatch $f
			i=$((i+1))
			
			# snooze after submitting 6 batches of exps
			if [[ $(($i % 6)) -eq 0 ]]; then
				echo $i "snooze 120s"
				sleep 120s
			else
				echo $i	
				if [[ $d == "retail" ]]; then
					sleep 15s
				else
					# 20k is very slow
					if [ $c -eq 20000 ]; then
						sleep 120s
					else
						sleep 60s
					fi
				fi
			fi
		done
	done
done

echo "$i batches of exps DONE!"
