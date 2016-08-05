#!/bin/bash
for sf in 1; do
	for type in ssbm-q01 ssbm-q01_eager; do
		./${type} ../../database/sf-${sf} >${type}-sf-${sf}.out
	done
done

