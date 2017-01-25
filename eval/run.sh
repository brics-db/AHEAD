#!/usr/bin/env bash

# Copyright (c) 2016-2017 Till Kolditz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Basic constants
CXX_COMPILER=g++-6
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
VARIANTS=("_normal" "_dmr_seq" "_dmr_mt" "_early" "_late" "_continuous" "_continuous_reenc")


# distinct warmup and test phases
ARGS=("$@")
if [[ $# -ne 0 ]] ; then
    case "${ARGS[0]}" in
        COMPILE)
            echo "COMPILE Phase"
			DO_COMPILE=1
			DO_COMPILE_CMAKE=1
			DO_BENCHMARK=0
			DO_EVAL=0
			DO_VERIFY=0
            ;;
        WARMUP)
            echo "WARMUP Phase"
			DO_COMPILE=0
			DO_BENCHMARK=1
			DO_EVAL=1
			DO_VERIFY=1
            ;;
        ACTUAL)
            echo "ACTUAL Phase"
			DO_COMPILE=0
			DO_BENCHMARK=1
			DO_EVAL=1
			DO_VERIFY=1
            ;;
		EVALONLY)
            echo "EVALONLY Phase"
			DO_COMPILE=0
			DO_BENCHMARK=0
			DO_EVAL=1
			DO_EVAL_PREPARE=1
			DO_VERIFY=1
			;;
        BATCH)
            /usr/bin/env $0 COMPILE
            for i in $(seq 1 1); do
                /usr/bin/env $0 ACTUAL
                mv ${PATH_EVALDATA} "${PATH_EVALDATA}_act${i}"
                mv ${PATH_EVALOUT} "${PATH_EVALOUT}_act${i}"
            done
            exit 0
			;;
		*)
            echo "UNKNOWN Phase"
			;;
    esac
else
	echo "DEFAULT Phase"
fi

# Process Switches
if [[ -z "$DO_COMPILE" ]]; then DO_COMPILE=1; fi # yes we want to set it either when it's unset or empty
if [[ -z "$DO_COMPILE_CMAKE" ]]; then DO_COMPILE_CMAKE=1; fi
if [[ -z "$DO_BENCHMARK" ]]; then DO_BENCHMARK=1; fi
if [[ -z "$DO_EVAL" ]]; then DO_EVAL=1; fi
if [[ -z "$DO_EVAL_PREPARE" ]]; then DO_EVAL_PREPARE=1; fi
if [[ -z "$DO_VERIFY" ]]; then DO_VERIFY=1; fi

# Process specific constants
if [[ -z "$CMAKE_BUILD_TYPE" ]]; then CMAKE_BUILD_TYPE=Release; fi

if [[ -z "$BENCHMARK_NUMRUNS" ]]; then BENCHMARK_NUMRUNS=10; fi # like above
if [[ -z "$BENCHMARK_NUMBEST" ]]; then BENCHMARK_NUMBEST=10; fi
declare -p BENCHMARK_SCALEFACTORS 2>/dev/null
ret=$?
if [[ $ret -ne 0 ]] || [[ -z "$BENCHMARK_SCALEFACTORS" ]]; then BENCHMARK_SCALEFACTORS=($(seq -s " " 1 10)); fi

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
set term pdf enhanced monochrome fontscale 0.44 size 3.25in,1.25in
set output '${2}'
set style data histogram
set style histogram cluster gap 1
#set style fill pattern 0 border 1
set auto x
unset key
$(for var in "${@:4}"; do echo $var; done)
plot '${3}' using 2:xtic(1) title col fs pattern 0 bo lw 1 dt 1, \\
        '' using 3:xtic(1) title col fs pattern 2 bo lw 1 dt 1, \\
        '' using 4:xtic(1) title col fs pattern 6 bo lw 1 dt 1, \\
        '' using 5:xtic(1) title col fs pattern 3 bo lw 1 dt 1, \\
        '' using 6:xtic(1) title col fs pattern 7 bo lw 1 dt 1, \\
        '' using 7:xtic(1) title col fs pattern 1 bo lw 1 dt 1, \\
        '' using 8:xtic(1) title col fs pattern 4 bo lw 1 dt 1
EOM
}

gnuplotlegend () {
        # Write GNUplot code to file
        cat >$1 << EOM
#!/usr/bin/env gnuplot
set term pdf enhanced monochrome fontscale 0.44 size 6in,0.2in
set output '${2}'
set datafile separator '\t'
set style data histogram
set style histogram cluster gap 1
set style fill pattern 0 border
set lmargin 0
set rmargin 0
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
set key below maxcols 5 maxrows 1 horizontal width 0.5
$(for var in "${@:5}"; do echo $var; done)
plot '${4}' using 2:xtic(1) t "Unencoded", \\
        '' using 3:xtic(1) t "Early", \\
        '' using 4:xtic(1) t "Late", \\
        '' using 5:xtic(1) t "Continuous", \\
        '' using 6:xtic(1) t "Reencoding", \\
        '' using 7:xtic(1) t "DMR Seq", \\
        '' using 8:xtic(1) t "DMR MT"
        
set term pdf enhanced monochrome fontscale 0.44 size 0.2in,1.25in
set output '${3}'
set lmargin 0
set rmargin 0
unset border
unset xtics
unset ytics
unset x2tics
unset y2tics
unset xlabel
unset x2label
unset ylabel
unset y2label
unset label
unset arrow
unset key
#set label 'Relative Throughput' at screen 0.5, bm + 0.4 * (size + gap) offset 0,-strlen("Relative Throughput")/4.0 rotate by 90
set label 'Relative Throughput' at screen 0.5,0.15 rotate by 90
plot 1 ls 0 with linespoints
EOM
}

#############################
# Actual Script starts here #
#############################

numcpus=$(nproc)
if [[ $? -ne 0 ]]; then
    numcpus=$(cat /proc/cpuinfo | grep processor | wc -l)
fi


# Compile
if [[ ${DO_COMPILE} -ne 0 ]]; then
    date
    if [[ ${DO_COMPILE_CMAKE} -ne 0 ]]; then
        echo "Recreating build dir \"${PATH_BUILD}\"."
        rm -Rf ${PATH_BUILD}
        mkdir -p ${PATH_BUILD}
        pushd ${PATH_BUILD}
        cxx=$(which ${CXX_COMPILER})
        if [[ -e ${cxx} ]]; then
            export CXX=$cxx
        else
            echo "This benchmark requires G++-6. You may modify variable CXX_COMPILER at the top of the script."
            exit 1
        fi
        cmake ${PATH_BASE} -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        exitcode=$?
        if [[ ${exitcode} -ne 0 ]]; then
            exit ${exitcode};
        fi
    else
        pushd ${PATH_BUILD}
    fi
    echo "Compiling."
    /bin/bash -c "make -j${numcpus}"
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
        for var in "${VARIANTS[@]}"; do
            type="${BASE2}${var}"
            PATH_BINARY=${PATH_BUILD}/${type}
            if [[ -e ${PATH_BINARY} ]]; then
                EVAL_FILEOUT="${PATH_EVALDATA}/${type}.out"
                EVAL_FILEERR="${PATH_EVALDATA}/${type}.err"
                EVAL_FILETIME="${PATH_EVALDATA}/${type}.time"
                rm -f ${EVAL_FILEOUT} ${EVAL_FILEERR} ${EVAL_FILETIME}
                echo -n " * ${type}:"
                for sf in ${BENCHMARK_SCALEFACTORS[*]}; do
                    echo -n " sf${sf}"
                    echo "Scale Factor ${sf} ===========================" >>${EVAL_FILETIME}
                    /usr/bin/time -v -o ${EVAL_FILETIME} -a $corenum ${PATH_BINARY} --numruns ${BENCHMARK_NUMRUNS} --verbose --print-result --dbpath ${PATH_DB}/sf-${sf} 1>>${EVAL_FILEOUT} 2>>${EVAL_FILEERR}
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

            rm -f ${EVAL_TEMPFILE}
            rm -f ${EVAL_DATAFILE}
            echo -n "SF " >${EVAL_TEMPFILE}
            echo "${BENCHMARK_SCALEFACTORS[*]}" >>${EVAL_TEMPFILE}

		for var in "${VARIANTS[@]}"; do
			type="${BASE2}${var}"
                EVAL_FILEOUT="${PATH_EVALDATA}/${type}.out"
                EVAL_FILERESULTS="${PATH_EVALDATA}/${type}.results"
                EVAL_FILESUMMARY="${PATH_EVALDATA}/${type}.summary"
                EVAL_FILEBESTRUNS="${PATH_EVALDATA}/${type}.bestruns"
                grep -o 'result.*$' ${EVAL_FILEOUT} >${EVAL_FILERESULTS}
                grep -A ${BENCHMARK_NUMRUNS} "TotalTimes" ${EVAL_FILEOUT} | sed '/^--$/d' | grep -v "TotalTimes:" >${EVAL_FILESUMMARY}
                count=0
                sfidx=0
                sf=${BENCHMARK_SCALEFACTORS[${sfidx}]}

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
                        ((sfidx++))
                        sf=${BENCHMARK_SCALEFACTORS[${sfidx}]}
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
            sfIdxs=$(seq -s " " 1 ${#BENCHMARK_SCALEFACTORS[@]})
            arg="FNR==NR {if (FNR==2) { "
            for sf in ${sfIdxs}; do # number of scale factors
                column=$(echo "${sf}+1" | bc)
                #arg+="max${sf}=(\$${column}+0>max${sf})?\$${column}:max${sf};"
                arg+="max${sf}=\$${column};"
            done
            arg+="};next} FNR==1 {print \$1"
            for sf in ${sfIdxs}; do
                column=$(echo "${sf}+1" | bc)
                arg+=",\$${column}"
            done
            arg+=";next} {print \$1"
            for sf in ${sfIdxs}; do # number of scale factors
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
        gnuplotcode ${BASE2}.m ${BASE2}.pdf ${BASE2}.data \
            "set yrange [0:*]" "set grid" "set xlabel 'Scale Factor'" "set ylabel 'Runtime [ns]'"

        gnuplotcode ${BASE2}.norm.m ${BASE2}.norm.pdf ${BASE2}.norm.data \
            "set yrange [0.9:2]" "set ytics out" "set xtics out" "set grid noxtics ytics" "unset xlabel" "unset ylabel"

		gnuplotlegend ${BASE2}.legend.m ${BASE2}.legend.pdf ${BASE2}.xlabel.pdf ${BASE2}.data

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
if [[ ${DO_VERIFY} -ne 0 ]]; then
    date
    echo "Verifying:"
    for NUM in "${IMPLEMENTED[@]}"; do
        echo -n " * Q${NUM}:"
        BASE2="${BASE}${NUM}"
        NUMVARS="${#VARIANTS[@]}"
        baseline1="${PATH_EVALDATA}/${BASE2}${VARIANTS[0]}.results"
        baseline1Tmp="${baseline1}.tmp"
        baseline2="${PATH_EVALDATA}/${BASE2}${VARIANTS[0]}.err"
        awk '{print $1,$2}' "${baseline1}" >"${baseline1Tmp}"
        for i in $(seq 1 $(echo "$NUMVARS-1"|bc)); do
            echo -n " ${VARIANTS[$i]:1}="
            other1="${PATH_EVALDATA}/${BASE2}${VARIANTS[$i]}.results"
            other1Tmp="${other1}.tmp"
            awk '{print $1,$2}' "${other1}" >"${other1Tmp}" # filters out e.g. the encoded value for the continuous encoding variants
            other2="${PATH_EVALDATA}/${BASE2}${VARIANTS[$i]}.err"
            RES1=$(diff "${baseline1Tmp}" "${other1Tmp}" | wc -l)
            RES2=$(diff "${baseline2}" "${other2}" | wc -l)
            if [[ "${RES1}" -eq 0 ]] && [[ "${RES2}" -eq 0 ]]; then
                echo -n "OK";
            else
                echo -n "BAD";
                if [[ "${RES2}" -eq 0 ]]; then echo -n "(result)"; else echo -n "(err)"; fi
            fi
        done
        echo ""
    done
else
    echo "Skipping verification."
fi
