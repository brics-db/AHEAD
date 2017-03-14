#/usr/bin/env /bin/bash

BINDIR=../build
DBDIR=../database/sf-1
NUMRUNS=1

echo "Running R-Store SSB for Verification. Using arguments '-d ${DBDIR} -n ${NUMRUNS} -p'..."

for q in 11 12 13 21; do
	strQ="${q}" 
	echo "  * Q ${strQ:0:1}.${strQ:1:1}:"
	for t in normal early late dmr_seq dmr_mt continuous continuous_reenc; do
		echo "    + ${t}"
		for s in seq SSE; do
			echo "      o ${s}"
			${BINDIR}/ssbm-q${q}_${t}_${s} -d ${DBDIR} -n ${NUMRUNS} -p 1>ssbm-q${q}_${t}_${s}.out 2>ssbm-q${q}_${t}_${s}.err
		done
	done
done

echo "Done."

