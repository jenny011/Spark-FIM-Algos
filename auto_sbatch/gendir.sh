#!/bash/bin

for d in retail kosarak chainstore record;
do
	for c in 0 20000 40000 60000 80000 100000;
	do
		for p in 4 8 12;
		do
			for m in 51 41 31 21 11 1;
			do
				mkdir "$d/perf/$m"_"$c"_"$p"
			done
		done
	done
done
