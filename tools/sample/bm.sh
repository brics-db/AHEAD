#!/bin/bash

#for sf in `seq 1 10`; do
#	for type in ssbm-q01 ssbm-q01_eager; do
#		./${type} ../../database/sf-${sf} >${type}-sf-${sf}.out
#	done
#done

echo "" >lineorder_size.out
for sf in `seq 1 10`; do
	./lineorder_size ../../database/sf-${sf} | tee -a lineorder_size.out
done

