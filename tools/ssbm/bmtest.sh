#!/usr/bin/env bash

#for sf in 1; do
for type in ssbm-q01 ssbm-q01_eager ssbm-q01_lazy ssbm-q01_encoded; do
	FILEOUT="${type}.out"
	FILEERR="${type}.err"
	echo "" >${FILEOUT}
	echo "" >${FILEERR}
	for sf in `seq 1 10`; do
		./${type} --dbpath ../../database/sf-${sf} 1>>${FILEOUT} 2>>${FILEERR}
#		./${type} --dbpath ../../database/sf-${sf}
	done
done

#echo "" >lineorder_size.out
#for sf in `seq 1 10`; do
#	./lineorder_size ../../database/sf-${sf} | tee -a lineorder_size.out
#done

