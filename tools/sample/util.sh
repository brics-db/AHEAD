#!/bin/bash

for f in ssbm-q01 ssbm-q01_eager ssbm-q01_lazy ssbm-q01_encoded; do
	grep -o 'result.*$' ${f}.out >${f}.result
	grep -A 10 "TotalTimes" ${f}.out >${f}.out.summary
done
#grep -o 'result.*$' ssbm-q01.out >ssbm-q01.result
#grep -o 'result.*$' ssbm-q01_eager.out >ssbm-q01_eager.result
#grep -o 'result.*$' ssbm-q01_lazy.out >ssbm-q01_lazy.result

DIFF1=$(if [[ $(diff ssbm-q01.result ssbm-q01_eager.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
DIFF2=$(if [[ $(diff ssbm-q01.result ssbm-q01_lazy.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
DIFF3=$(if [[ $(diff ssbm-q01.result ssbm-q01_encoded.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
DIFF4=$(if [[ $(diff ssbm-q01_lazy.result ssbm-q01_eager.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
DIFF5=$(if [[ $(diff ssbm-q01_lazy.result ssbm-q01_encoded.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
DIFF6=$(if [[ $(diff ssbm-q01_eager.result ssbm-q01_encoded.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)

if [[ DIFF1 -eq "OK" ]] && [[ DIFF2 -eq "OK" ]] && [[ DIFF3 -eq "OK" ]] && [[ DIFF4 -eq "OK" ]] && [[ DIFF5 -eq "OK" ]] && [[ DIFF6 -eq "OK" ]]; then
	echo "all OK"
	exit 0;
fi

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

#echo "" >lineorder_size.out; for sf in `seq 1 10`; do date +%T | tr -d '\n'; echo -n ": Scale Factor ${sf}... "; echo "Scale Factor ${sf}" >>lineorder_size.out; ./lineorder_size ../../database/sf-${sf} >>lineorder_size.out; date +%T | tr -d '\n'; echo ": Done!"; done

