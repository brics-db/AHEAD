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
VARREPLACE="s/_//g"
IMPLEMENTED=(11 12 13 21)
VARIANTS=("_normal" "_dmr_seq" "_dmr_mt" "_early" "_late" "_continuous" "_continuous_reenc")
ARCHITECTURE=("_seq")
cat /proc/cpuinfo | grep sse4_2 &>/dev/null
HAS_SSE42=$?
if [[ ${HAS_SSE42} -eq 0 ]]; then ARCHITECTURE+=("_SSE"); fi
#cat /proc/cpuinfo | grep avx2 &>/dev/null
#HAS_AVX2=$?
#if [[ ${HAS_AVX2} -eq 0 ]]; then ARCHITECTURE+="_AVX2"; fi
## For the following, keep in mind that AVX-512 has several sub-sets and not all of them may be available
#cat /proc/cpuinfo | grep avx512 &>/dev/null
#HAS_AVX512=$?
#if [[ ${HAS_AVX512} -eq 0 ]]; then ARCHITECTURE+="_AVX512"; fi

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
            exit 1
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
set term pdf enhanced color fontscale 0.44 size 3.25in,1.25in
set output '${2}'
set style data histogram
set style histogram cluster gap 1
set auto x
unset key
set style line 1 lc rgb "#9400d3"
set style line 2 lc rgb "#009e73"
set style line 3 lc rgb "#56b4e9"
set style line 4 lc rgb "#e69f00"
set style line 5 lc rgb "#000000"
set style line 6 lc rgb "#0072b2"
set style line 7 lc rgb "#e51e10"
$(for var in "${@:4}"; do echo $var; done)
plot '${3}' using 2:xtic(1) title col fillstyle pattern 0 border ls 1 lw 1 dt 1, \\
         '' using 3:xtic(1) title col fillstyle pattern 2 border ls 2 lw 1 dt 1, \\
         '' using 5:xtic(1) title col fillstyle pattern 3 border ls 4 lw 1 dt 1, \\
         '' using 6:xtic(1) title col fillstyle pattern 7 border ls 5 lw 1 dt 1, \\
         '' using 7:xtic(1) title col fillstyle pattern 1 border ls 6 lw 1 dt 1, \\
         '' using 8:xtic(1) title col fillstyle pattern 4 border ls 7 lw 1 dt 1
#         '' using 4:xtic(1) title col fillstyle pattern 6 border ls 3 lw 1 dt 1, \\
EOM
}

gnuplotlegend () {
        # Write GNUplot code to file
        cat >$1 << EOM
#!/usr/bin/env gnuplot
set term pdf enhanced color fontscale 0.44 size 3.25in,0.3in
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
set key below horizontal
set style line 1 lc rgb "#9400d3"
set style line 2 lc rgb "#009e73"
set style line 3 lc rgb "#56b4e9"
set style line 4 lc rgb "#e69f00"
set style line 5 lc rgb "#000000"
set style line 6 lc rgb "#0072b2"
set style line 7 lc rgb "#e51e10"
$(for var in "${@:5}"; do echo $var; done)
plot '${4}' using 2:xtic(1) fillstyle pattern 0 border ls 1 lw 1 dt 1 t "Unencoded", \\
         '' using 3:xtic(1) fillstyle pattern 2 border ls 2 lw 1 dt 1 t "DMR", \\
         '' using 5:xtic(1) fillstyle pattern 3 border ls 4 lw 1 dt 1 t "Early", \\
         '' using 6:xtic(1) fillstyle pattern 7 border ls 5 lw 1 dt 1 t "Late", \\
         '' using 7:xtic(1) fillstyle pattern 1 border ls 6 lw 1 dt 1 t "Continuous", \\
         '' using 8:xtic(1) fillstyle pattern 4 border ls 7 lw 1 dt 1 t "Reencoding"
#         '' using 4:xtic(1) fillstyle pattern 6 border ls 3 lw 1 dt 1 t "DMR MT", \\

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
    echo "Compiling:"
    if [[ ${DO_COMPILE_CMAKE} -ne 0 ]]; then
        echo " * Recreating build dir \"${PATH_BUILD}\"."
        rm -Rf ${PATH_BUILD}
        mkdir -p ${PATH_BUILD}
        pushd ${PATH_BUILD}
        cxx=$(which ${CXX_COMPILER})
        if [[ -e ${cxx} ]]; then
            export CXX=$cxx
        else
            echo " ! This benchmark requires G++-6. You may modify variable CXX_COMPILER at the top of the script."
            exit 1
        fi
        echo " * Running cmake (Build Type = ${CMAKE_BUILD_TYPE}"
        cmake ${PATH_BASE} -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        exitcode=$?
        if [[ ${exitcode} -ne 0 ]]; then
            echo "    ! Error! Exitcode = ${exitcode}"
            exit ${exitcode};
        fi
    else
        pushd ${PATH_BUILD}
    fi
    echo " * Running make (-j${numcpus})"
    /bin/bash -c "make -j${numcpus}"
    exitcode=$?
    popd
    if [[ ${exitcode} -ne 0 ]]; then
        echo "    ! Error! Exitcode = ${exitcode}"
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

    for ARCH in "${ARCHITECTURE[@]}"; do
        for NUM in "${IMPLEMENTED[@]}"; do
            BASE2=${BASE}${NUM}
            for VAR in "${VARIANTS[@]}"; do
                type="${BASE2}${VAR}${ARCH}"
                PATH_BINARY=${PATH_BUILD}/${type}
                if [[ -e ${PATH_BINARY} ]]; then
                    EVAL_FILEOUT="${PATH_EVALDATA}/${type}.out"
                    EVAL_FILEERR="${PATH_EVALDATA}/${type}.err"
                    EVAL_FILETIME="${PATH_EVALDATA}/${type}.time"
                    rm -f ${EVAL_FILEOUT} ${EVAL_FILEERR} ${EVAL_FILETIME}
                    echo -n " * ${type}:"
                    for SF in ${BENCHMARK_SCALEFACTORS[*]}; do
                        echo -n " sf${SF}"
                        echo "Scale Factor ${SF} ===========================" >>${EVAL_FILETIME}
                        /usr/bin/time -avo ${EVAL_FILETIME} ${PATH_BINARY} --numruns ${BENCHMARK_NUMRUNS} --verbose --print-result --dbpath ${PATH_DB}/sf-${SF} 1>>${EVAL_FILEOUT} 2>>${EVAL_FILEERR}
                    done
                    echo " done."
                else
                    echo " * Skipping missing binary \"${type}\"."
                fi
            done
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

    for ARCH in "${ARCHITECTURE[@]}"; do
        # Prepare File for a single complete normalized overhead graph across all scale factors
        EVAL_NORMALIZEDALLDATAFILE="norm-all${ARCH}.data"
        EVAL_NORMALIZEDALLDATAFILE_PATH="${PATH_EVALOUT}/${EVAL_NORMALIZEDALLDATAFILE}"
        EVAL_NORMALIZEDALLPLOTFILE="norm-all${ARCH}.m"
        EVAL_NORMALIZEDALLPLOTFILE_PATH="${PATH_EVALOUT}/${EVAL_NORMALIZEDALLPLOTFILE}"
        EVAL_NORMALIZEDALLPDFFILE="norm-all${ARCH}.pdf"
        EVAL_NORMALIZEDALLPDFFILE_PATH="${PATH_EVALOUT}/${EVAL_NORMALIZEDALLPDFFILE}"
        if [[ ${DO_EVAL_PREPARE} -ne 0 ]]; then
            echo " * ${ARCH:1}"
            rm -f ${EVAL_NORMALIZEDALLDATAFILE_PATH}
            echo -n "Query" >>${EVAL_NORMALIZEDALLDATAFILE_PATH}
            for var in "${VARIANTS[@]}"; do
                echo -n " ${var}" >>${EVAL_NORMALIZEDALLDATAFILE_PATH}
            done
            echo "" >>${EVAL_NORMALIZEDALLDATAFILE_PATH}
        fi

        for NUM in "${IMPLEMENTED[@]}"; do
            BASE2="${BASE}${NUM}"
            BASE3="${BASE2}${ARCH}"
            EVAL_TEMPFILE="${BASE3}.tmp"
            EVAL_TEMPFILE_PATH="${PATH_EVALDATA}/${EVAL_TEMPFILE}"
            EVAL_DATAFILE="${BASE3}.data"
            EVAL_DATAFILE_PATH="${PATH_EVALOUT}/${EVAL_DATAFILE}"
            EVAL_NORMALIZEDTEMPFILE="${BASE3}-norm.tmp"
            EVAL_NORMALIZEDTEMPFILE_PATH="${PATH_EVALDATA}/${EVAL_NORMALIZEDTEMPFILE}"
            EVAL_NORMALIZEDDATAFILE="${BASE3}-norm.data"
            EVAL_NORMALIZEDDATAFILE_PATH="${PATH_EVALOUT}/${EVAL_NORMALIZEDDATAFILE}"

            if [[ ${DO_EVAL_PREPARE} -ne 0 ]]; then
                echo "   * Preparing data for ${BASE3}"

                rm -f ${EVAL_TEMPFILE_PATH}
                rm -f ${EVAL_DATAFILE_PATH}
                echo -n "SF " >${EVAL_TEMPFILE_PATH}
                echo "${BENCHMARK_SCALEFACTORS[*]}" >>${EVAL_TEMPFILE_PATH}

                for VAR in "${VARIANTS[@]}"; do
                    type="${BASE2}${VAR}${ARCH}"
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
                    echo -n "${type}" >>${EVAL_TEMPFILE_PATH}

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
                            echo -n " ${arithmean}" >>${EVAL_TEMPFILE_PATH}
                            count=0
                            unset array
                            array=()
                            ((sfidx++))
                            sf=${BENCHMARK_SCALEFACTORS[${sfidx}]}
                        fi
                    done
                    echo "" >>${EVAL_TEMPFILE_PATH}
                done

                # transpose original data
                awktranspose ${EVAL_TEMPFILE_PATH} ${EVAL_DATAFILE_PATH}
                if [[ ${BASEREPLACE1} ]]; then
                    sed -i -e ${BASEREPLACE1} ${EVAL_DATAFILE_PATH}
                    sed -i -e ${BASEREPLACE2} ${EVAL_DATAFILE_PATH}
                fi

                # prepare awk statement to normalize all columns and output them to the normalized temp file
                # 2016-11-04: normalize to "normal" (unencoded) base variant
                sfIdxs=$(seq -s " " 1 ${#BENCHMARK_SCALEFACTORS[@]})
                arg="FNR==NR {if (FNR==2) { "
                for sf in ${sfIdxs}; do # number of scale factors
                    column=$(echo "${sf}+1" | bc)
                    #arg+="max${sf}=(\$${column}+0>max${sf})?\$${column}:max${sf};"
                    arg+="norm${sf}=\$${column};"
                done
                arg+="};next} FNR==1 {print \$1"
                for sf in ${sfIdxs}; do
                    column=$(echo "${sf}+1" | bc)
                    arg+=",\$${column}"
                done
                arg+=";next} {print \$1"
                for sf in ${sfIdxs}; do # number of scale factors
                    column=$(echo "${sf}+1" | bc)
                    arg+=",\$${column}/norm${sf}"
                done
                arg+="}"
                # EVAL_TEMPFILE_PATH already contains the average (arithmetic mean) of the best X of Y runs
                # (see BENCHMARK_NUMRUNS and BENCHMARK_NUMBEST)
                awk "${arg}" ${EVAL_TEMPFILE_PATH} ${EVAL_TEMPFILE_PATH} >${EVAL_NORMALIZEDTEMPFILE_PATH}

                # transpose normalized data
                awktranspose ${EVAL_NORMALIZEDTEMPFILE_PATH} ${EVAL_NORMALIZEDDATAFILE_PATH}
                if [[ ${BASEREPLACE1} ]]; then
                    sed -i -e ${BASEREPLACE1} ${EVAL_NORMALIZEDDATAFILE_PATH}
                    sed -i -e ${BASEREPLACE2} ${EVAL_NORMALIZEDDATAFILE_PATH}
                    tr <${EVAL_NORMALIZEDDATAFILE_PATH} -d '\000' >${EVAL_NORMALIZEDDATAFILE_PATH}.tmp
                    mv ${EVAL_NORMALIZEDDATAFILE_PATH}.tmp ${EVAL_NORMALIZEDDATAFILE_PATH}
                fi

                # Append current Query name to normalized overall temp file
                echo -n $(echo -n "${NUM} " | sed 's/\([0-9]\)\([0-9]\)/Q\1.\2/g') >>${EVAL_NORMALIZEDALLDATAFILE_PATH}
                echo -n " " >>${EVAL_NORMALIZEDALLDATAFILE_PATH}
                #prepare awk statement to generate from the normalized temp file the normalized data across all scalefactors
                sfIdxs=$(seq -s " " 1 ${#BENCHMARK_SCALEFACTORS[@]})
                varIdxs=$(seq -s " " 1 ${#VARIANTS[@]})
                arg="BEGIN {ORS=\" \"} NR==FNR { if (FNR==1) {next} {norm[FNR]=(("
                for sf in ${sfIdxs}; do
                    column=$(echo "${sf}+1" | bc)
                    arg+="\$${column}+"
                done
                arg+="0)/${#BENCHMARK_SCALEFACTORS[@]})};next} FNR==1 {next} {print norm[FNR]}"
                awk "${arg}" ${EVAL_NORMALIZEDTEMPFILE_PATH} ${EVAL_NORMALIZEDTEMPFILE_PATH} >>${EVAL_NORMALIZEDALLDATAFILE_PATH}
                echo "" >>${EVAL_NORMALIZEDALLDATAFILE_PATH}
                sed -i -e ${VARREPLACE} ${EVAL_NORMALIZEDALLDATAFILE_PATH}
                # remove <zero>-bytes
                tr <${EVAL_NORMALIZEDALLDATAFILE_PATH} -d '\000' >${EVAL_NORMALIZEDALLDATAFILE_PATH}.tmp
                mv ${EVAL_NORMALIZEDALLDATAFILE_PATH}.tmp ${EVAL_NORMALIZEDALLDATAFILE_PATH}
            fi

            echo "   * Plotting ${BASE3}"
            pushd ${PATH_EVALOUT}
            #gnuplotcode <output file> <gnuplot target output file> <gnuplot data file>
            gnuplotcode ${BASE3}.m ${BASE3}.pdf ${EVAL_DATAFILE} \
                "set yrange [0:*]" "set grid" "set xlabel 'Scale Factor'" "set ylabel 'Runtime [ns]'"

            gnuplotcode ${BASE3}-norm.m ${BASE3}-norm.pdf ${EVAL_NORMALIZEDDATAFILE} \
                "set yrange [0.9:2]" "set ytics out" "set xtics out" "set grid noxtics ytics" "unset xlabel" "unset ylabel"

            gnuplotlegend ${BASE3}-legend.m ${BASE3}-legend.pdf ${BASE3}-xlabel.pdf ${BASE3}.data

            gnuplot ${BASE3}.m
            gnuplot ${BASE3}-norm.m
            gnuplot ${BASE3}-legend.m
            popd
        done

        echo "   * Creating PDF file with all diagrams (${ALLPDFOUTFILE})"
        ALLPDFINFILES=
        for NUM in "${IMPLEMENTED[@]}"; do
            BASE3=${BASE}${NUM}${ARCH}
            ALLPDFINFILES+=" ${PATH_EVALOUT}/${BASE3}.pdf ${PATH_EVALOUT}/${BASE3}-norm.pdf"
        done
        ALLPDFOUTFILE=${PATH_EVALOUT}/ssbm-all${ARCH}.pdf
        gs -sDEVICE=pdfwrite -dCompatibilityLevel=1.4 -dPDFSETTINGS=/default -dNOPAUSE -dQUIET -dBATCH -dDetectDuplicateImages -dCompressFonts=true -r150 -sOutputFile=${ALLPDFOUTFILE} ${ALLPDFINFILES}
        # gs -sDEVICE=pdfwrite -dCompatibilityLevel=1.4 -dPDFSETTINGS=/default -dNOPAUSE -dQUIET -dBATCH -dDetectDuplicateImages -dCompressFonts=true -r150 -sOutputFile=output.pdf input.pdf

        gnuplotcode  ${EVAL_NORMALIZEDALLPLOTFILE_PATH} ${EVAL_NORMALIZEDALLPDFFILE_PATH} ${EVAL_NORMALIZEDALLDATAFILE_PATH} \
            "set yrange [0.9:]" "set ytics out" "set xtics out" "set grid noxtics ytics" "unset xlabel" "unset ylabel"
        gnuplot ${EVAL_NORMALIZEDALLPLOTFILE_PATH}
    done
else
    echo "Skipping evaluation."
fi

# Verification
if [[ ${DO_VERIFY} -ne 0 ]]; then
    date
    echo "Verifying:"
    for ARCH in "${ARCHITECTURE[@]}"; do
        echo " * ${ARCH}"
        for NUM in "${IMPLEMENTED[@]}"; do
            echo -n "   * Q${NUM}:"
            BASE2="${BASE}${NUM}"
            NUMVARS="${#VARIANTS[@]}"
            baseline1="${PATH_EVALDATA}/${BASE2}${VARIANTS[0]}${ARCH}.results"
            baseline1Tmp="${baseline1}.tmp"
            baseline2="${PATH_EVALDATA}/${BASE2}${VARIANTS[0]}${ARCH}.err"
            awk '{print $1,$2}' "${baseline1}" >"${baseline1Tmp}"
            for i in $(seq 1 $(echo "$NUMVARS-1"|bc)); do
                echo -n " ${VARIANTS[$i]:1}="
                other1="${PATH_EVALDATA}/${BASE2}${VARIANTS[$i]}${ARCH}.results"
                other1Tmp="${other1}.tmp"
                awk '{print $1,$2}' "${other1}" >"${other1Tmp}" # filters out e.g. the encoded value for the continuous encoding variants
                other2="${PATH_EVALDATA}/${BASE2}${VARIANTS[$i]}${ARCH}.err"
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
    done
else
    echo "Skipping verification."
fi
