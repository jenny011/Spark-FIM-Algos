#!/bin/bash

if [ ! -d slurms ]; then
	mkdir slurms
fi

for i in 0 1 2 3 4 5 6;
do
	if [ ! -d slurms/slurms$i ]; then
		mkdir slurms/slurms$i
	fi
	for d in retail kosarak chainstore record;
	do
		if [ ! -d slurms/slurms$i/$d ]; then
			mkdir slurms/slurms$i/$d
		fi
		for ((c=0;c<=100000;c+=20000));
		do
			if [ ! -d slurms/slurms$i/$d/$c ]; then
				mkdir slurms/slurms$i/$d/$c
			fi
		done
	done
done

python gen.py

