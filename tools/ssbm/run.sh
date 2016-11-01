#!/usr/bin/env bash

# Benchmarking
BASE=ssbm-q
BASEREPLACE1="s/${BASE}\([0-9]\)\([0-9]\)/Q\1.\2/g"
BASEREPLACE2="s/[_]\([^[:space:]]\)[^[:space:]]*/^\{\1\}/g"
IMPLEMENTED=(11 12 13)
DBLOC=../database

DO_BENCH=0
DO_VERIFY=0
DO_EVAL=1

NUMRUNS=10
NUMBEST=7

SFMIN=1
SFMAX=1

EVALDIR=eval

rm -rf ${EVALDIR}
mkdir ${EVALDIR}

function awktranspose {
    # awk code taken verbatim from:
    # http://stackoverflow.com/questions/1729824/transpose-a-file-in-bash
    awk '
    { 
        for (i=1; i<=NF; i++)  {
            a[NR,i] = $i
        }
    }
    NF>p { p = NF }
    END {    
        for(j=1; j<=p; j++) {
            str=a[1,j]
            for(i=2; i<=NR; i++){
                str=str" "a[i,j];
            }
            print str
        }
    }' $1 >$2
}

# Benchmarking
if [[ ${DO_BENCH} -eq 1 ]]; then
    echo "Benchmarking:"
    for NUM in "${IMPLEMENTED[@]}"; do
        BASE2=${BASE}${NUM}
        for type in ${BASE2} ${BASE2}_early ${BASE2}_late ${BASE2}_continuous; do
            if [[ -e ${type} ]]; then
                FILEOUT="${type}.out"
                FILEERR="${type}.err"
                rm -f ${FILEOUT}
                rm -f ${FILEERR}
                echo -n "${type}:"
                for sf in $(seq ${SFMIN} ${SFMAX}); do
                    echo -n " sf${sf}"
                    ./${type} --numruns ${NUMRUNS} --dbpath ${DBLOC}/sf-${sf} 1>>${FILEOUT} 2>>${FILEERR}
                done
                echo " done."
            fi
        done
    done
else
    echo "Skipping benchmarks."
fi

# Evaluation
if [[ ${DO_EVAL} -eq 1 ]]; then
    echo "Evaluating"
    array=()

    for NUM in "${IMPLEMENTED[@]}"; do
        BASE2=${BASE}${NUM}

        for sf in $(seq ${SFMIN} ${SFMAX}); do
            rm -f ${BASE2}.tmp
            rm -f ${BASE2}.data
            rm -f ${BASE2}.pdf
        done
        echo -n "SF " >${EVALDIR}/${BASE2}.tmp
        echo $(seq ${SFMIN} ${SFMAX}) >>${EVALDIR}/${BASE2}.tmp

        for f in ${BASE2} ${BASE2}_early ${BASE2}_late ${BASE2}_continuous; do
            grep -o 'result.*$' ${f}.out >${EVALDIR}/${f}.results
            grep -A 10 "TotalTimes" ${f}.out | sed '/^--$/d' | grep -v "TotalTimes:" >${EVALDIR}/${f}.summary
            count=0
            sf=${SFMIN}

            rm -f ${EVALDIR}/${f}.bestruns
            echo -n ${f} >>${EVALDIR}/${BASE2}.tmp

            for i in $(awk '{print $2;}' ${EVALDIR}/${f}.summary); do
                array+=($i)
                ((count++))
                if [[ ${count} -eq ${NUMRUNS} ]]; then
                    # a batch of ${NUMRUNS} runs, i.e. all runs of a scale factor
                    # 1) compute the best runs (i.e. remove outliers)
                    bestruns=$(printf "%s\n" "${array[@]}" | sort -n | head -n ${NUMBEST} | tr '\n' ' ')
                    echo "SF ${sf}: ${bestruns[@]}" >>${EVALDIR}/${f}.bestruns
                    # 2) compute the arithmetic mean
                    total=0
                    IFS=', ' read -r -a array <<< "$bestruns"
                    for k in "${array[@]}"; do
                        total=$(echo "$total + $k" | bc)
                    done
                    arithmean=$(echo "scale=0; ${total} / ${NUMBEST}" | bc)
                    # 3) append to file
                    echo -n " ${arithmean}" >>${EVALDIR}/${BASE2}.tmp
                    count=0
                    unset array
                    array=()
                    ((sf++))
                fi
            done
            echo "" >>${EVALDIR}/${BASE2}.tmp
        done

        # transpose, write gnuplot file and generate diagram
        awktranspose ${EVALDIR}/${BASE2}.tmp ${EVALDIR}/${BASE2}.data
        if [[ ${BASEREPLACE1} ]]; then
            sed -i -e ${BASEREPLACE1} ${EVALDIR}/${BASE2}.data
            sed -i -e ${BASEREPLACE2} ${EVALDIR}/${BASE2}.data
        fi
        cat >${EVALDIR}/${BASE2}.gnuplot <<- EOM
            #!/usr/bin/env gnuplot

            #set terminal pdf enhanced monochrome
            set terminal pdf monochrome
            set output '${BASE2}.pdf'

            set style data histogram
            set style histogram cluster gap 1

            #set style fill solid border rgb "black"
            set style fill pattern border
            set auto x
            set yrange [0:*]
            plot '${BASE2}.data' using 2:xtic(1) title col, \
                    '' using 3:xtic(1) title col, \
                    '' using 4:xtic(1) title col, \
                    '' using 5:xtic(1) title col
EOM
        cd ${EVALDIR}; gnuplot ${BASE2}.gnuplot; cd ..
    done
else
    echo "Skipping evaluation."
fi

# Verification
EXITSTAT=0
if [[ ${DO_VERIFY} -eq 1 ]]; then
    echo "Verifying:"
    for NUM in "${IMPLEMENTED[@]}"; do
        DIFF1=$(if [[ $(diff ssbm-q${NUM}.result ssbm-q${NUM}_early.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
        DIFF2=$(if [[ $(diff ssbm-q${NUM}.result ssbm-q${NUM}_late.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
        DIFF3=$(if [[ $(diff ssbm-q${NUM}.result ssbm-q${NUM}_continuous.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
        DIFF4=$(if [[ $(diff ssbm-q${NUM}_late.result ssbm-q${NUM}_early.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
        DIFF5=$(if [[ $(diff ssbm-q${NUM}_late.result ssbm-q${NUM}_continuous.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
        DIFF6=$(if [[ $(diff ssbm-q${NUM}_early.result ssbm-q${NUM}_continuous.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)

        if [[ DIFF1 -eq "OK" ]] && [[ DIFF2 -eq "OK" ]] && [[ DIFF3 -eq "OK" ]] && [[ DIFF4 -eq "OK" ]] && [[ DIFF5 -eq "OK" ]] && [[ DIFF6 -eq "OK" ]]; then
            echo "Q${NUM}: all OK"
        elif [[ DIFF1 -eq "OK" ]]; then
            if [[ DIFF2 -eq "OK" ]]; then
                echo "Q${NUM}: All produce the same result"
            else
                echo "Only ssbm-q${NUM} and ssbm-q${NUM}_early produce the same result"
                ++${EXITSTAT}
            fi
        else
            if [[ DIFF2 -eq "OK" ]]; then
                echo "Only ssbm-q${NUM} and ssbm-q${NUM}_late produce the same result"
                ++${EXITSTAT}
            elif [[ DIFF3 -eq "OK" ]]; then
                echo "Only ssbm-q${NUM}_late and ssbm-q${NUM}_early produce the same result"
                ++${EXITSTAT}
            fi
        fi
    done
else
    echo "Skipping verification."
fi

exit ${EXITSTAT}
