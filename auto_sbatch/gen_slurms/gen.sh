#!/bin/bash

if [ ! -d slurms ]; then
	mkdir slurms
fi

for i in 0 1 2 3 4 5 6;
do
	if [ ! -d slurms/slurms$i ]; then
		mkdir slurms/slurms$i
	fi
done

python gen.py

