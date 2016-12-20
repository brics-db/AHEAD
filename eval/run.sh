#!/usr/bin/env bash

# Basic constants
PATH_BASE=..
PATH_BUILD=${PATH_BASE}/build
PATH_DB=${PATH_BASE}/database
PATH_EVAL=${PATH_BASE}/eval
PATH_EVALDATA=${PATH_EVAL}/data
PATH_EVALOUT=${PATH_EVAL}/out
BASE=ssbm-q
BASEREPLACE1="s/${BASE}\([0-9]\)\([0-9]\)/Q\1.\2/g"
BASEREPLACE2="s/[_]\([^[:space:]]\)[^[:space:]]*/^\{\1\}/g"
IMPLEMENTED=(11 12 13 21)

# Process Switches
#DO_CLEAN_EVALTEMP=0
if [[ -z "$DO_COMPILE" ]]; then DO_COMPILE=1; fi # yes we want to set it either when it's unset or empty
if [[ -z "$DO_COMPILE_CMAKE" ]]; then DO_COMPILE_CMAKE=0; fi
if [[ -z "$DO_BENCHMARK" ]]; then DO_BENCHMARK=1; fi
if [[ -z "$DO_EVAL" ]]; then DO_EVAL=1; fi
if [[ -z "$DO_EVAL_PREPARE" ]]; then DO_EVAL_PREPARE=1; fi
if [[ -z "$DO_VERIFY" ]]; then DO_VERIFY=1; fi

# Process specific constants
CMAKE_BUILD_TYPE=release

BENCHMARK_NUMRUNS=3
BENCHMARK_NUMBEST=2
BENCHMARK_SFMIN=1
BENCHMARK_SFMAX=1

# functions etc
pushd () {
    command pushd "$@" > /dev/null
}

popd () {
    command popd "$@" > /dev/null
}

date () {
    /bin/bash -c 'date "+%Y-%m-%d %H-%M-%S"'
}

#########################################################
# awktranspose                                          #
#                                                       #
# arg1) input file                                      #
# arg2) output file                                     #
#########################################################
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
                str=str"\t"a[i,j];
            }
            print str
        }
    }' $1 >$2
}

#########################################################
# gnuplotcode                                           #
#                                                       #
# arg1) <code output file>                              #
#       where the generated code should written be to   #
# arg2) <gnuplot output file>                           #
#       where gnuplot should store its result           #
# arg3) <gnuplot input file>                            #
#       location of gnuplot data file                   #
# arg4+) <gnuplot custom code>                          #
#       custom code to be added                         #
#########################################################
gnuplotcode () {
        # Write GNUplot code to file
        cat >$1 << EOM
#!/usr/bin/env gnuplot
#reset
set term pdf enhanced monochrome
#set term pdf enhanced
set output '${2}'
set style data histogram
set style histogram cluster gap 1
#set style fill solid border rgb "black"
set style fill transparent pattern 0.5 border
set auto x
#set key right outside
unset key
$(for var in "${@:4}"; do echo $var; done)
plot '${3}' using 2:xtic(1) title col, \\
        '' using 3:xtic(1) title col, \\
        '' using 4:xtic(1) title col, \\
        '' using 5:xtic(1) title col
EOM
}

gnuplotlegend () {
        # Write GNUplot code to file
        cat >$1 << EOM
#!/usr/bin/env gnuplot
#reset
set term pdf enhanced monochrome size 6.7in,0.2in
#set term pdf enhanced
set output '${2}'
set datafile separator '\t'
set style data histogram
set style histogram cluster gap 1
set style fill transparent pattern 0.5 border
unset border
unset xtics
unset xlabel
unset x2tics
unset x2label
unset ytics
unset ylabel
unset y2tics
unset y2label
set xrange [-10:0]
set yrange [-10:0]
set key below center
$(for var in "${@:4}"; do echo $var; done)
plot '${3}' using 2:xtic(1) t "Unencoded", \\
        '' using 3:xtic(1) t "Early", \\
        '' using 4:xtic(1) t "Late", \\
        '' using 5:xtic(1) t "Continuous"
EOM
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
    date    
    echo "Compiling."
    pushd ${PATH_BUILD}
    if [[ ${DO_COMPILE_CMAKE} -ne 0 ]]; then
        cmake ${PATH_BASE} -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        exitcode=$?
        if [[ ${exitcode} -ne 0 ]]; then
            exit ${exitcode};
        fi
    fi
    make
    exitcode=$?
    popd
    if [[ ${exitcode} -ne 0 ]]; then
        exit ${exitcode};
    fi
else
    echo "Skipping compilation."
fi

# Benchmarking
if [[ ${DO_BENCHMARK} -ne 0 ]]; then
    date
    echo "Benchmarking:"

    if [[ ! -d ${PATH_EVALDATA} ]]; then
        mkdir -p ${PATH_EVALDATA}
    fi

    for NUM in "${IMPLEMENTED[@]}"; do
        BASE2=${BASE}${NUM}
        for type in ${BASE2} ${BASE2}_early ${BASE2}_late ${BASE2}_continuous ${BASE2}_continuous_reenc; do
            PATH_BINARY=${PATH_BUILD}/${type}
            if [[ -e ${PATH_BINARY} ]]; then
                EVAL_FILEOUT="${PATH_EVALDATA}/${type}.out"
                EVAL_FILEERR="${PATH_EVALDATA}/${type}.err"
                rm -f ${EVAL_FILEOUT}
                rm -f ${EVAL_FILEERR}
                echo -n " * ${type}:"
                for sf in $(seq ${BENCHMARK_SFMIN} ${BENCHMARK_SFMAX}); do
                    echo -n " sf${sf}"
                    env ${PATH_BINARY} --numruns ${BENCHMARK_NUMRUNS} --dbpath ${PATH_DB}/sf-${sf} 1>>${EVAL_FILEOUT} 2>>${EVAL_FILEERR}
                done
                echo " done."
            else
                echo " * Skipping missing binary \"${type}\"."
            fi
        done
    done
else
    echo "Skipping benchmarks."
fi

# Evaluation
if [[ ${DO_EVAL} -ne 0 ]]; then
    date
    echo "Evaluating"
    array=()

    if [[ ! -d ${PATH_EVALDATA} ]]; then
        mkdir -p ${PATH_EVALDATA}
    fi
    if [[ ! -d ${PATH_EVALOUT} ]]; then
        mkdir -p ${PATH_EVALOUT}
    fi

    for NUM in "${IMPLEMENTED[@]}"; do
        BASE2=${BASE}${NUM}
        EVAL_TEMPFILE=${PATH_EVALDATA}/${BASE2}.tmp
        EVAL_DATAFILE=${PATH_EVALOUT}/${BASE2}.data
        EVAL_NORMALIZEDTEMPFILE=${PATH_EVALDATA}/${BASE2}.norm.tmp
        EVAL_NORMALIZEDDATAFILE=${PATH_EVALOUT}/${BASE2}.norm.data

        if [[ ${DO_EVAL_PREPARE} -ne 0 ]]; then
            echo " * Preparing data for ${BASE2}"

            #for sf in $(seq ${BENCHMARK_SFMIN} ${BENCHMARK_SFMAX}); do
                rm -f ${EVAL_TEMPFILE}
                rm -f ${EVAL_DATAFILE}
            #done
            echo -n "SF " >${EVAL_TEMPFILE}
            echo $(seq ${BENCHMARK_SFMIN} ${BENCHMARK_SFMAX}) >>${EVAL_TEMPFILE}

            for type in ${BASE2} ${BASE2}_early ${BASE2}_late ${BASE2}_continuous ${BASE2}_continuous_reenc; do
                EVAL_FILEOUT="${PATH_EVALDATA}/${type}.out"
                EVAL_FILERESULTS="${PATH_EVALDATA}/${type}.results"
                EVAL_FILESUMMARY="${PATH_EVALDATA}/${type}.summary"
                EVAL_FILEBESTRUNS="${PATH_EVALDATA}/${type}.bestruns"
                grep -o 'result.*$' ${EVAL_FILEOUT} >${EVAL_FILERESULTS}
                grep -A ${BENCHMARK_NUMRUNS} "TotalTimes" ${EVAL_FILEOUT} | sed '/^--$/d' | grep -v "TotalTimes:" >${EVAL_FILESUMMARY}
                count=0
                sf=${BENCHMARK_SFMIN}

                rm -f ${EVAL_FILEBESTRUNS}
                echo -n "${type}" >>${EVAL_TEMPFILE}

                for i in $(awk '{print $2;}' ${EVAL_FILESUMMARY}); do
                    array+=($i)
                    ((count++))
                    if [[ ${count} -eq ${BENCHMARK_NUMRUNS} ]]; then
                        # a batch of ${BENCHMARK_NUMRUNS} runs, i.e. all runs of a scale factor
                        # 1) compute the best runs (i.e. remove outliers)
                        bestruns=$(printf "%s\n" "${array[@]}" | sort -n | head -n ${BENCHMARK_NUMBEST} | tr '\n' ' ')
                        echo "SF ${sf}: ${bestruns[@]}" >>${EVAL_FILEBESTRUNS}
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

            # transpose original data
            awktranspose ${EVAL_TEMPFILE} ${EVAL_DATAFILE}
            if [[ ${BASEREPLACE1} ]]; then
                sed -i -e ${BASEREPLACE1} ${EVAL_DATAFILE}
                sed -i -e ${BASEREPLACE2} ${EVAL_DATAFILE}
            fi

            # prepare awk statement to normalize all columns and output them to the normalized temp file
            # 2016-11-04: normalize to "normal" (unencoded) base variant
            arg="FNR==NR {if (FNR==2) { "
            for sf in $(seq ${BENCHMARK_SFMIN} ${BENCHMARK_SFMAX}); do # number of scale factors
                column=$(echo "${sf}+1" | bc)
                #arg+="max${sf}=(\$${column}+0>max${sf})?\$${column}:max${sf};"
                arg+="max${sf}=\$${column};"
            done
            arg+="};next} FNR==1 {print \$1"
            for sf in $(seq ${BENCHMARK_SFMIN} ${BENCHMARK_SFMAX}); do
                column=$(echo "${sf}+1" | bc)
                arg+=",\$${column}"
            done
            arg+=";next} {print \$1"
            for sf in $(seq ${BENCHMARK_SFMIN} ${BENCHMARK_SFMAX}); do # number of scale factors
                column=$(echo "${sf}+1" | bc)
                arg+=",\$${column}/max${sf}"
            done
            arg+="}"
            # EVAL_TEMPFILE already contains the average (arithmetic mean) of the best X of Y runs
            # (see BENCHMARK_NUMRUNS and BENCHMARK_NUMBEST)
            awk "${arg}" ${EVAL_TEMPFILE} ${EVAL_TEMPFILE} >${EVAL_NORMALIZEDTEMPFILE}

            # transpose normalized data
            awktranspose ${EVAL_NORMALIZEDTEMPFILE} ${EVAL_NORMALIZEDDATAFILE}
            if [[ ${BASEREPLACE1} ]]; then
                sed -i -e ${BASEREPLACE1} ${EVAL_NORMALIZEDDATAFILE}
                sed -i -e ${BASEREPLACE2} ${EVAL_NORMALIZEDDATAFILE}
            fi
        fi

        echo " * Plotting ${BASE2}"
		pushd ${PATH_EVALOUT}
        #gnuplotcode <output file> <gnuplot target output file> <gnuplot data file>
        gnuplotcode ${BASE2}.m ${BASE2}.pdf ${BASE2}.data "set yrange [0:*]" "set grid" "set xlabel 'Scale Factor'" "set ylabel 'Runtime [ns]'"

        gnuplotcode ${BASE2}.norm.m ${BASE2}.norm.pdf ${BASE2}.norm.data "set yrange [0:1.5]" "set grid" "set xlabel 'Scale Factor'" "set ylabel 'Normalized Runtime'"

		gnuplotlegend ${BASE2}.legend.m ${BASE2}.legend.pdf ${BASE2}.data

        gnuplot ${BASE2}.m
        gnuplot ${BASE2}.norm.m
		gnuplot ${BASE2}.legend.m

		popd
    done

    echo " * Creating PDF file with all diagrams (${ALLPDFOUTFILE})"
    ALLPDFINFILES=
    for NUM in "${IMPLEMENTED[@]}"; do
        BASE2=${BASE}${NUM}
        ALLPDFINFILES+=" ${PATH_EVALOUT}/${BASE2}.pdf ${PATH_EVALOUT}/${BASE2}.norm.pdf"
    done
    ALLPDFOUTFILE=${PATH_EVALOUT}/${BASE}.pdf
    gs -sDEVICE=pdfwrite -dCompatibilityLevel=1.4 -dPDFSETTINGS=/default -dNOPAUSE -dQUIET -dBATCH -dDetectDuplicateImages -dCompressFonts=true -r150 -sOutputFile=${ALLPDFOUTFILE} ${ALLPDFINFILES}
    # gs -sDEVICE=pdfwrite -dCompatibilityLevel=1.4 -dPDFSETTINGS=/default -dNOPAUSE -dQUIET -dBATCH -dDetectDuplicateImages -dCompressFonts=true -r150 -sOutputFile=output.pdf input.pdf
else
    echo "Skipping evaluation."
fi

# Verification
EXITSTAT=0
if [[ ${DO_VERIFY} -ne 0 ]]; then
    date
    echo "Verifying:"
    for NUM in "${IMPLEMENTED[@]}"; do
        BASE2=${BASE}${NUM}
        normal=${PATH_EVALDATA}/${BASE2}.results
        early=${PATH_EVALDATA}/${BASE2}_early.results
        late=${PATH_EVALDATA}/${BASE2}_late.results
        contin=${PATH_EVALDATA}/${BASE2}_continuous.results
        reenc=${PATH_EVALDATA}/${BASE2}_continuous_reenc.results
        DIFF1=$(if [[ $(diff ${normal} ${early}  | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
        DIFF2=$(if [[ $(diff ${normal} ${late}   | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
        DIFF3=$(if [[ $(cat ${contin} | awk '{print $1,$2}' | diff ${normal} - | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
        DIFF4=$(if [[ $(diff ${early}  ${late}   | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
        DIFF5=$(if [[ $(cat ${contin} | awk '{print $1,$2}' | diff ${early}  - | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
        DIFF6=$(if [[ $(cat ${contin} | awk '{print $1,$2}' | diff ${late}   - | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
		DIFF7=$(if [[ $(cat ${contin} | awk '{print $1,$2}' | diff ${reenc}   - | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
        if [[ ${DIFF1} == "OK" ]] && [[ ${DIFF2} == "OK" ]] && [[ ${DIFF3} == "OK" ]] && [[ ${DIFF4} == "OK" ]] && [[ ${DIFF5} == "OK" ]] && [[ ${DIFF6} == "OK" ]]; then
            echo " * Q${NUM}: all OK"
        elif [[ ${DIFF1} == "OK" ]]; then
            if [[ ${DIFF2} == "OK" ]]; then
                echo " * Q${NUM}: normal, early and late produce the same result"
                ((++EXITSTAT))
            elif [[ ${DIFF3} == "OK" ]]; then
                echo " * Q${NUM}: normal, early and continuous produce the same result"
                ((++EXITSTAT))
            else
                echo " * Q${NUM}: normal and early produce the same result"
                ((++EXITSTAT))
            fi
        elif [[ ${DIFF2} == "OK" ]]; then
            if [[ ${DIFF3} == "OK" ]]; then
                echo " * Q${NUM}: normal, late and continuous produce the same result"
                ((++EXITSTAT))
            else
                echo " * Q${NUM}: normal and late produce the same result"
                ((++EXITSTAT))
            fi
        elif [[ ${DIFF3} == "OK" ]]; then
            echo " * Q${NUM}: normal and continuous produce the same result"
            ((++EXITSTAT))
        else
            echo " * Q${NUM}: no results match"
            ((++EXITSTAT))
        fi
    done
else
    echo "Skipping verification."
fi

exit ${EXITSTAT}
