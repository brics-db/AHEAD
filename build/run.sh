#!/bin/bash

# Benchmarking
BASE=ssbm-q
IMPLEMENTED=(11 12)

for NUM in "${IMPLEMENTED[@]}"; do
	BASE2=${BASE}${NUM}
	for type in ${BASE2} ${BASE2}_eager ${BASE2}_lazy ${BASE2}_encoded; do
		FILEOUT="${type}.out"
		FILEERR="${type}.err"
		echo "" >${FILEOUT}
		echo "" >${FILEERR}
#		for sf in `seq 1 10`; do
		for sf in 1; do
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

EXITSTAT=0
for NUM in "${IMPLEMENTED[@]}"; do
    DIFF1=$(if [[ $(diff ssbm-q${NUM}.result ssbm-q${NUM}_eager.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
    DIFF2=$(if [[ $(diff ssbm-q${NUM}.result ssbm-q${NUM}_lazy.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
    DIFF3=$(if [[ $(diff ssbm-q${NUM}.result ssbm-q${NUM}_encoded.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
    DIFF4=$(if [[ $(diff ssbm-q${NUM}_lazy.result ssbm-q${NUM}_eager.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
    DIFF5=$(if [[ $(diff ssbm-q${NUM}_lazy.result ssbm-q${NUM}_encoded.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
    DIFF6=$(if [[ $(diff ssbm-q${NUM}_eager.result ssbm-q${NUM}_encoded.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)

    if [[ DIFF1 -eq "OK" ]] && [[ DIFF2 -eq "OK" ]] && [[ DIFF3 -eq "OK" ]] && [[ DIFF4 -eq "OK" ]] && [[ DIFF5 -eq "OK" ]] && [[ DIFF6 -eq "OK" ]]; then
            echo "Q${NUM}: all OK"
    elif [[ DIFF1 -eq "OK" ]]; then
            if [[ DIFF2 -eq "OK" ]]; then
                    echo "Q${NUM}: All produce the same result"
            else
                    echo "Only ssbm-q${NUM} and ssbm-q${NUM}_eager produce the same result"
                    ++${EXITSTAT}
            fi
    else
            if [[ DIFF2 -eq "OK" ]]; then
                    echo "Only ssbm-q${NUM} and ssbm-q${NUM}_lazy produce the same result"
                    ++${EXITSTAT}
            elif [[ DIFF3 -eq "OK" ]]; then
                    echo "Only ssbm-q${NUM}_lazy and ssbm-q${NUM}_eager produce the same result"
                    ++${EXITSTAT}
            fi
    fi
done
exit ${EXITSTAT}

#echo "" >lineorder_size.out; for sf in `seq 1 10`; do date +%T | tr -d '\n'; echo -n ": Scale Factor ${sf}... "; echo "Scale Factor ${sf}" >>lineorder_size.out; ./lineorder_size ../../database/sf-${sf} >>lineorder_size.out; date +%T | tr -d '\n'; echo ": Done!"; done

