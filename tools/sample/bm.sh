#!/bin/bash

echo "" >bm.out
echo "" >bm.err

#for sf in `seq 1 10`; do
for sf in 1; do
	for type in ssbm-q01 ssbm-q01_eager ssbm-q01_lazy; do
#		./${type} ../../database/sf-${sf} >${type}-sf-${sf}.out
		./${type} ../../database/sf-${sf} 1>>bm.out 2>>bm.err
	done
done

#echo "" >lineorder_size.out
#for sf in `seq 1 10`; do
#	./lineorder_size ../../database/sf-${sf} | tee -a lineorder_size.out
#done

