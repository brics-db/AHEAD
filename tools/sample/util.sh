#!/bin/bash

grep -o 'result.*$' ssbm-q01.out >ssbm-q01.result
grep -o 'result.*$' ssbm-q01_eager.out >ssbm-q01_eager.result
grep -o 'result.*$' ssbm-q01_lazy.out >ssbm-q01_lazy.result

DIFF1=$(if [[ $(diff ssbm-q01.result ssbm-q01_eager.result | wc -l) -eq 0 ]]; then echo "OK"; else echo "FAIL"; fi)
DIFF2=$(if [[ $(diff ssbm-q01.result ssbm-q01_lazy.result | wc -l) -eq 0 ]]; then echo "OK"; else echo "FAIL"; fi)
DIFF3=$(if [[ $(diff ssbm-q01_lazy.result ssbm-q01_eager.result | wc -l) -eq 0 ]]; then echo "OK"; else echo "FAIL"; fi)

if [[ DIFF1 -eq "OK" ]]; then
	if [[ DIFF2 -eq "OK" ]]; then
		echo "All produce the same result"
		exit 0
	else
		echo "Only ssbm-q01 and ssbm-q01_eager produce the same result"
		exit 1
	fi
else
	if [[ DIFF2 -eq "OK" ]]; then
		echo "Only ssbm-q01 and ssbm-q01_lazy produce the same result"
		exit 2
	elif [[ DIFF3 -eq "OK" ]]; then
		echo "Only ssbm-q01_lazy and ssbm-q01_eager produce the same result"
		exit 3
	fi
fi

