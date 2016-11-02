#!/usr/bin/env bash

# Basic constants
PATH_BASE=..
PATH_BUILD=${PATH_BASE}/build
PATH_DB=${PATH_BASE}/database
PATH_EVALDATA=data
PATH_EVALOUT=out
BASE=ssbm-q
BASEREPLACE1="s/${BASE}\([0-9]\)\([0-9]\)/Q\1.\2/g"
BASEREPLACE2="s/[_]\([^[:space:]]\)[^[:space:]]*/^\{\1\}/g"
IMPLEMENTED=(11 12 13)

# Process Switches
#DO_CLEAN_EVALTEMP=0
DO_COMPILE=0
DO_COMPILE_CMAKE=0
DO_BENCHMARK=0
DO_EVAL=1
DO_EVAL_PREPARE=1
DO_VERIFY=0

# Process specific constants
CMAKE_BUILD_TYPE=release

BENCHMARK_NUMRUNS=10
BENCHMARK_NUMBEST=7
BENCHMARK_SFMIN=1
BENCHMARK_SFMAX=1

# functions etc
pushd () {
    command pushd "$@" > /dev/null
}

popd () {
    command popd "$@" > /dev/null
}

awktranspose () {
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

#############################
# Actual Script starts here #
#############################

# Clean temp dir
#if [[ DO_CLEAN_EVALTEMP -ne 0 ]]; then
#    echo "Cleaning temp dir \"${PATH_EVALDATA}\"."
#    rm -rf ${PATH_EVALDATA}
#    mkdir ${PATH_EVALDATA}
#else
#    echo "Skipping cleaning."
#fi

# Compile
if [[ ${DO_COMPILE} -ne 0 ]]; then
    echo "Compiling."
    if [[ ${DO_COMPILE_CMAKE} -ne 0 ]]; then
        pushd ${PATH_BASE}; cmake . -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}; popd
    fi
    pushd ${PATH_BASE}; make; popd
else
    echo "Skipping compilation."
fi

# Benchmarking
if [[ ${DO_BENCHMARK} -ne 0 ]]; then
    echo "Benchmarking:"
    for NUM in "${IMPLEMENTED[@]}"; do
        BASE2=${BASE}${NUM}
        for type in ${BASE2} ${BASE2}_early ${BASE2}_late ${BASE2}_continuous; do
            PATH_BINARY=${PATH_BUILD}/${type}
            if [[ -e ${PATH_BINARY} ]]; then
                EVAL_FILEOUT="${PATH_EVALDATA}/${type}.out"
                EVAL_FILEERR="${PATH_EVALDATA}/${type}.err"
DO_EVAL=1
                rm -f ${EVAL_FILEOUT}
                rm -f ${EVAL_FILEERR}
                echo -n " * ${type}:"
                for sf in $(seq ${BENCHMARK_SFMIN} ${BENCHMARK_SFMAX}); do
                    echo -n " sf${sf}"
                    env ${PATH_BINARY} --numruns ${BENCHMARK_NUMRUNS} --dbpath ${PATH_DB}/sf-${sf} 1>>${EVAL_FILEOUT} 2>>${EVAL_FILEERR}
                done
                echo " done."
            else
                echo " * Skipping missing binary \"${PATH_BINARY}\"."
            fi
        done
    done
else
    echo "Skipping benchmarks."
fi

# Evaluation
if [[ ${DO_EVAL} -ne 0 ]]; then
    echo "Evaluating"
    array=()

    if [[ ! -d ${PATH_EVALDATA} ]]; then
        mkdir ${PATH_EVALDATA}
    fi
    if [[ ! -d ${PATH_EVALOUT} ]]; then
        mkdir ${PATH_EVALOUT}
    fi

    for NUM in "${IMPLEMENTED[@]}"; do
        BASE2=${BASE}${NUM}
        EVAL_TEMPFILE=${PATH_EVALDATA}/${BASE2}.tmp
        EVAL_DATAFILE=${PATH_EVALOUT}/${BASE2}.data
        if [[ ${DO_EVAL_PREPARE} -ne 0 ]]; then
            echo " * Preparing data for ${BASE2}"

            #for sf in $(seq ${BENCHMARK_SFMIN} ${BENCHMARK_SFMAX}); do
                rm -f ${EVAL_TEMPFILE}
                rm -f ${EVAL_DATAFILE}
            #done
            echo -n "SF " >${EVAL_TEMPFILE}
            echo $(seq ${BENCHMARK_SFMIN} ${BENCHMARK_SFMAX}) >>${EVAL_TEMPFILE}

            for type in ${BASE2} ${BASE2}_early ${BASE2}_late ${BASE2}_continuous; do
                EVAL_FILEOUT="${PATH_EVALDATA}/${type}.out"
                EVAL_FILERESULTS="${PATH_EVALDATA}/${type}.results"
                EVAL_FILESUMMARY="${PATH_EVALDATA}/${type}.summary"
                EVAL_FILEBESTRUNS="${PATH_EVALDATA}/${type}.bestruns"
                grep -o 'result.*$' ${EVAL_FILEOUT} >${EVAL_FILERESULTS}
                grep -A 10 "TotalTimes" ${EVAL_FILEOUT} | sed '/^--$/d' | grep -v "TotalTimes:" >${EVAL_FILESUMMARY}
                count=0
                sf=${BENCHMARK_SFMIN}

                rm -f ${EVAL_FILEBESTRUNS}
                echo -n ${type} >>${EVAL_TEMPFILE}

                for i in $(awk '{print $2;}' ${EVAL_FILESUMMARY}); do
                    array+=($i)
                    ((count++))
                    if [[ ${count} -eq ${BENCHMARK_NUMRUNS} ]]; then
                        # a batch of ${BENCHMARK_NUMRUNS} runs, i.e. all runs of a scale factor
                        # 1) compute the best runs (i.e. remove outliers)
                        bestruns=$(printf "%s\n" "${array[@]}" | sort -n | head -n ${BENCHMARK_NUMBEST} | tr '\n' ' ')
                        echo "SF {sf}: ${bestruns[@]}" >>${EVAL_FILEBESTRUNS}
                        # 2) compute the arithmetic mean
                        total=0
                        IFS=', ' read -r -a array <<< "$bestruns"
                        for k in "${array[@]}"; do
                            total=$(echo "$total + $k" | bc)
                        done
                        arithmean=$(echo "scale=0; ${total} / ${BENCHMARK_NUMBEST}" | bc)
                        # 3) append to file
                        echo -n " ${arithmean}" >>${EVAL_TEMPFILE}
                        count=0
                        unset array
                        array=()
                        ((sf++))
                    fi
                done
                echo "" >>${EVAL_TEMPFILE}
            done

            # transpose, write gnuplot file and generate diagram
            awktranspose ${EVAL_TEMPFILE} ${EVAL_DATAFILE}
            if [[ ${BASEREPLACE1} ]]; then
                sed -i -e ${BASEREPLACE1} ${EVAL_DATAFILE}
                sed -i -e ${BASEREPLACE2} ${EVAL_DATAFILE}
            fi
        fi

        echo " * Plotting ${BASE2}"
        EVAL_PLOTFILE=${BASE2}.gnuplot
        EVAL_PLOTOUTFILE=${BASE2}.pdf

        # Write GNUplot code to file
        cat >${PATH_EVALOUT}/${EVAL_PLOTFILE} << EOM
#!/usr/bin/env gnuplot

set terminal pdf enhanced monochrome
#set term png medium enhanced background "white"
          # x000000 x202020 x404040 x606060 \\
          # x808080 xA0A0A0 xC0C0C0 xE0E0E0
# white for the transparent background, black for borders, dark gray for axes, and a gray-scale for the six plotting colors.
set output '${EVAL_PLOTOUTFILE}'

set style data histogram
set style histogram cluster gap 1

#set style fill solid border rgb "black"
set style fill pattern border
set auto x
set yrange [0:*]
plot '${BASE2}.data' using 2:xtic(1) title col, \\
        '' using 3:xtic(1) title col, \\
        '' using 4:xtic(1) title col, \\
        '' using 5:xtic(1) title col
EOM

        pushd ${PATH_EVALOUT}; gnuplot ${EVAL_PLOTFILE}; popd
    done
else
    echo "Skipping evaluation."
fi

# Verification
EXITSTAT=0
if [[ ${DO_VERIFY} -ne 0 ]]; then
    echo "Verifying:"
    for NUM in "${IMPLEMENTED[@]}"; do
        DIFF1=$(if [[ $(diff ssbm-q${NUM}.result ssbm-q${NUM}_early.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
        DIFF2=$(if [[ $(diff ssbm-q${NUM}.result ssbm-q${NUM}_late.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
        DIFF3=$(if [[ $(diff ssbm-q${NUM}.result ssbm-q${NUM}_continuous.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
        DIFF4=$(if [[ $(diff ssbm-q${NUM}_late.result ssbm-q${NUM}_early.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
        DIFF5=$(if [[ $(diff ssbm-q${NUM}_late.result ssbm-q${NUM}_continuous.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
        DIFF6=$(if [[ $(diff ssbm-q${NUM}_early.result ssbm-q${NUM}_continuous.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)

        if [[ DIFF1 -eq "OK" ]] && [[ DIFF2 -eq "OK" ]] && [[ DIFF3 -eq "OK" ]] && [[ DIFF4 -eq "OK" ]] && [[ DIFF5 -eq "OK" ]] && [[ DIFF6 -eq "OK" ]]; then
            echo " * Q${NUM}: all OK"
        elif [[ DIFF1 -eq "OK" ]]; then
            if [[ DIFF2 -eq "OK" ]]; then
                echo " * Q${NUM}: All produce the same result"
            else
                echo " * Q${NUM}: Only ssbm-q${NUM} and ssbm-q${NUM}_early produce the same result"
                ++${EXITSTAT}
            fi
        else
            if [[ DIFF2 -eq "OK" ]]; then
                echo " * Q${NUM}: Only ssbm-q${NUM} and ssbm-q${NUM}_late produce the same result"
                ++${EXITSTAT}
            elif [[ DIFF3 -eq "OK" ]]; then
                echo " * Q${NUM}: Only ssbm-q${NUM}_late and ssbm-q${NUM}_early produce the same result"
                ++${EXITSTAT}
            fi
        fi
    done
else
    echo "Skipping verification."
fi

exit ${EXITSTAT}
