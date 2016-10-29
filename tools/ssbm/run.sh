#!/bin/bash

# Benchmarking
BASE=ssbm-q
IMPLEMENTED=(11 12 13)
for NUM in "${IMPLEMENTED[@]}"; do
	BASE2=${BASE}${NUM}
	for type in ${BASE2} ${BASE2}_eager ${BASE2}_lazy ${BASE2}_encoded; do
		FILEOUT="${type}.out"
		FILEERR="${type}.err"
		echo "" >${FILEOUT}
		echo "" >${FILEERR}
		for sf in `seq 1 10`; do
			./${type} ../../database/sf-${sf} 1>>${FILEOUT} 2>>${FILEERR}
#			./${type} ../../database/sf-${sf}
		done
	done
done

# Results preparations
for NUM in "${IMPLEMENTED[@]}"; do
        BASE2=${BASE}${NUM}
	for f in ${BASE2} ${BASE2}_eager ${BASE2}_lazy ${BASE2}_encoded; do
		grep -o 'result.*$' ${f}.out >${f}.result
		grep -A 10 "TotalTimes" ${f}.out >${f}.out.summary
	done
done

DIFF1=$(if [[ $(diff ssbm-q11.result ssbm-q11_eager.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
DIFF2=$(if [[ $(diff ssbm-q11.result ssbm-q11_lazy.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
DIFF3=$(if [[ $(diff ssbm-q11.result ssbm-q11_encoded.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
DIFF4=$(if [[ $(diff ssbm-q11_lazy.result ssbm-q11_eager.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
DIFF5=$(if [[ $(diff ssbm-q11_lazy.result ssbm-q11_encoded.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
DIFF6=$(if [[ $(diff ssbm-q11_eager.result ssbm-q11_encoded.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)

if [[ DIFF1 -eq "OK" ]] && [[ DIFF2 -eq "OK" ]] && [[ DIFF3 -eq "OK" ]] && [[ DIFF4 -eq "OK" ]] && [[ DIFF5 -eq "OK" ]] && [[ DIFF6 -eq "OK" ]]; then
	echo "all OK"
	exit 0;
fi

if [[ DIFF1 -eq "OK" ]]; then
	if [[ DIFF2 -eq "OK" ]]; then
		echo "All produce the same result"
		exit 0
	else
		echo "Only ssbm-q11 and ssbm-q11_eager produce the same result"
		exit 1
	fi
else
	if [[ DIFF2 -eq "OK" ]]; then
		echo "Only ssbm-q11 and ssbm-q11_lazy produce the same result"
		exit 2
	elif [[ DIFF3 -eq "OK" ]]; then
		echo "Only ssbm-q11_lazy and ssbm-q11_eager produce the same result"
		exit 3
	fi
fi

#echo "" >lineorder_size.out; for sf in `seq 1 10`; do date +%T | tr -d '\n'; echo -n ": Scale Factor ${sf}... "; echo "Scale Factor ${sf}" >>lineorder_size.out; ./lineorder_size ../../database/sf-${sf} >>lineorder_size.out; date +%T | tr -d '\n'; echo ": Done!"; done

