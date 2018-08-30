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

#####################
#####################
### Preliminaries ###
#####################
#####################

if [[ $(id -u) -eq 0 ]]; then
	echo "[ERROR] You must not run this script as super user!"
	exit 1
fi

##############################################
# Argument Processing for pre-defined phases #
##############################################
ARGS=("$@")
if [[ $# -ne 0 ]] ; then
	case "${ARGS[0]}" in
		ALL)
			PHASE="ALL"
			DO_COMPILE=1
			DO_COMPILE_CMAKE=1
			DO_BENCHMARK=1
			DO_EVAL=1
			DO_EVAL_PREPARE=1
			DO_VERIFY=1
			;;
		COMPILE)
			PHASE="COMPILE"
			DO_COMPILE=1
			DO_COMPILE_CMAKE=1
			DO_BENCHMARK=0
			DO_EVAL=0
			DO_EVAL_PREPARE=0
			DO_VERIFY=0
			;;
		ACTUAL)
			PHASE="ACTUAL"
			DO_COMPILE=0
			DO_BENCHMARK=1
			DO_EVAL=1
			DO_EVAL_PREPARE=1
			DO_VERIFY=1
			;;
		EVAL)
			PHASE="EVAL"
			DO_COMPILE=0
			DO_BENCHMARK=0
			DO_EVAL=1
			DO_EVAL_PREPARE=1
			DO_VERIFY=1
			if [[ $# > 1 ]]; then
				DATE="${ARGS[1]}" #only needed when calling the original script.
			fi
			;;
		PLOT)
			PHASE="PLOT"
			DO_COMPILE=0
			DO_BENCHMARK=0
			DO_EVAL=1
			DO_EVAL_PREPARE=0
			DO_VERIFY=0
			if [[ $# > 1 ]]; then
				DATE="${ARGS[1]}" #only needed when calling the original script.
			fi
			;;
		*)
			echo "[ERROR] UNKNOWN Phase"
			exit 1
			;;
	esac
else
	PHASE="DEFAULT"
fi ### [[ $# -ne 0 ]]

#######################
# Bootstrap Variables #
#######################
echo "DATE: ${DATE}"
CONFIG_FILE=run.conf
source "${CONFIG_FILE}"
echo "DATE: ${DATE}"

mkdir -p "${PATH_EVAL_CURRENT}"

################################################################################################
# if the outputs are not redirected to files, then call ourselves again with additional piping #
################################################################################################
if [[ -t 1 ]] && [[ -t 2 ]]; then
	filebase=$(basename -s '.sh' $0)
	outfile="${PATH_EVAL_CURRENT}/${filebase}.out"
	errfile="${PATH_EVAL_CURRENT}/${filebase}.err"
	if [[ -e ${PATH_EVAL_CURRENT} ]]; then
		if [[ -f "${outfile}" ]]; then
			idx=0
			while [[ -f "${outfile}.${idx}" ]]; do
				((idx++))
			done
			# keep both file versions in sync
			mv "${outfile}" "${outfile}.${idx}"
			mv "${errfile}" "${errfile}.${idx}"
		fi
	fi ### [[ -e ${PATH_EVAL_CURRENT} ]]
	rm -f $outfile $errfile
	echo "[INFO] one of stdout or stderr is not redirected, calling script with redirecting" | tee $outfile
	./$0 $@ >> >(tee "${outfile}") 2>> >(tee "${errfile}" >&2)
	ret=$?
	if [[ ${ret} != 0 ]]; then
		echo "[ERROR] Aborted. Exit code = $ret" >>$errfile
	else
		echo "[INFO] Finished" >>$outfile
	fi
	exit $ret
else
	echo "[INFO] stdout and stderr are redirected. Starting script. Parameters are: \"$@\""
fi ### [[ -t 1 ]] && [[ -t 2 ]]

# copy the script file to the sub-eval-folder and disable / change some lines to only enable data evaluation at a later time (e.g. to adapt the gnuplot scripts)
sed -E -e '34,+25s/^(.+)$/#\1/' -e '88s/^.+$/\tPHASE="EVALONLY"\n\tDO_COMPILE=0\n\tDO_BENCHMARK=0\n\tDO_EVAL=1\n\tDO_EVAL_PREPARE=1\n\tDO_VERIFY=1/' -e '94,2s/([^=]+)=(.+)/#\1=\2\n\1=./' -e '94s/^(.+)$/export DATE="'"${DATE}"'"\nexport PATH_EVAL_CURRENT="."\n\1/' -e '134,+2s/^(.+)$/#\1/' $0 >"${PATH_EVAL_CURRENT}/$(basename $0)"
sed -E -e '7s/^(\s+source\s+)(\.\.\/common.conf)$/\1..\/\2/' -e '9s/^(.+)$/#\1/' "${CONFIG_FILE}" >"${PATH_EVAL_CURRENT}/${CONFIG_FILE}"
chmod +x "${PATH_EVAL_CURRENT}/$(basename $0)"

echo "[INFO] Running Phase \"${PHASE}\""

if [[ -z ${reproscript+x} ]]; then
	echo "###########################################################"
	echo "# For the following tests, for better reproducibilty, we: #"
	echo "#   *  DISABLE turboboost                                 #"
	echo "#   * set the OS scaling governor to PERFORMANCE          #"
	echo "#                                                         #"
	echo "# For that, you need a sudoer account!                    #"
	echo "#                                                         #"
	echo -n "#   * turboboost: "
	if [[ $(sudo ./turboboost.sh disable &>/dev/null) ]]; then
		echo "succeeded.                              #"
	else
		echo "failed.                                 #"
	fi
	echo -n "#   * scaling governor: "
	modes=($(sudo $(pwd)/$(dirname $0)/../scalinggovernor.sh avail 0))
	hasperformance=0
	for mode in "${modes[@]}"; do
		if [[ "${mode}" == performance ]]; then
			hasperformance=1
			if [[ $(sudo ./scalinggovernor.sh set performance &>/dev/null) ]]; then 
				echo "succeeded.                        #"
			else
				echo "failed.                           #"
			fi
			break
		fi
	done
	[[ $hasperformance == 0 ]] && echo "failed. Did not find governor.    #"
	echo "###########################################################"
	echo
fi

###################
# Basic Variables #
###################
[ -z "$CXX_COMPILER" ] && CXX_COMPILER="c++"
[ -z "$GXX_COMPILER" ] && GXX_COMPILER="g++"
BASE=ssbm-q
BASEREPLACE1="s/${BASE}\([0-9]\)\([0-9]\)/Q\1.\2/g"
BASEREPLACE2="s/[_]\([^[:space:]]\)[^[:space:]]*/^\{\1\}/g"
VARREPLACE="s/_//g"
IMPLEMENTED=(11 12 13 21 22 23 31 32 33 34 41 42 43)
VARIANTS=("_normal" "_dmr_seq" "_early" "_late" "_continuous" "_continuous_reenc")
VARIANT_NAMES=("Unprotected" "DMR" "Early" "Late" "Continuous" "Reencoding")
ARCHITECTURE=("_scalar")
ARCHITECTURE_NAMES=("Scalar")
cat /proc/cpuinfo | grep sse4_2 &>/dev/null
HAS_SSE42=$?
if [[ ${HAS_SSE42} -eq 0 ]]; then
	ARCHITECTURE+=("_SSE");
	ARCHITECTURE_NAMES+=("SSE4.2")
fi

####################
# Process Switches #
####################
[ -z ${DO_COMPILE+x} ] && DO_COMPILE=1 # yes we want to set it either when it's unset or empty
[ -z ${DO_COMPILE_CMAKE+x} ] && DO_COMPILE_CMAKE=0 # do not re-generate cmake files be default every time
[ -z ${DO_BENCHMARK+x} ] && DO_BENCHMARK=1
[ -z ${DO_EVAL+x} ] && DO_EVAL=1
[ -z ${DO_EVAL_PREPARE+x} ] && DO_EVAL_PREPARE=1
[ -z ${DO_VERIFY+x} ] && DO_VERIFY=1

##############################
# Process specific constants #
##############################
### Compilation
[ -z ${CMAKE_BUILD_TYPE+x} ] && CMAKE_BUILD_TYPE=Release

### Benchmarking
[ -z ${BENCHMARK_NUMRUNS+x} ] && BENCHMARK_NUMRUNS=10 # like above
#[ -z ${BENCHMARK_NUMBEST+x} ] && BENCHMARK_NUMBEST=$(($BENCHMARK_NUMRUNS > 10 ? 10 : $BENCHMARK_NUMRUNS))
BENCHMARK_NUMBEST=$BENCHMARK_NUMRUNS
declare -p BENCHMARK_SCALEFACTORS &>/dev/null
ret=$?
( [ $ret -ne 0 ] || [ -z ${BENCHMARK_SCALEFACTORS+x} ] ) && BENCHMARK_SCALEFACTORS=($(seq -s " " ${AHEAD_SCALEFACTOR_MIN} ${AHEAD_SCALEFACTOR_MAX}))
[ -z ${BENCHMARK_DBDIR_SUFFIX+x} ] && BENCHMARK_DBDIR_SUFFIX= #"-restiny32"
[ -z ${BENCHMARK_MINBFW+x} ] && BENCHMARK_MINBFW= #1

### Eval
EVAL_TOTALRUNS_PER_VARIANT=$(echo "$BENCHMARK_NUMRUNS * ${#BENCHMARK_SCALEFACTORS[@]}"|bc)

#################
# functions etc #
#################
pushd () {
	command pushd "$@" &>/dev/null
}

popd () {
	command popd &>/dev/null
}

date () {
	${EXEC_ENV} ${EXEC_BASH} -c 'date "+%Y-%m-%d %H-%M-%S"'
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
# gnuplotcode[pdf|tex]                                  #
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
gnuplotcodepdf () {
	# Write GNUplot code to file
	cat >$1 << EOM
#!/usr/bin/env gnuplot
set term pdf enhanced color fontscale 0.44 size 5.7in,1.25in
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
		 '' using 4:xtic(1) title col fillstyle pattern 3 border ls 4 lw 1 dt 1, \\
		 '' using 5:xtic(1) title col fillstyle pattern 7 border ls 5 lw 1 dt 1, \\
		 '' using 6:xtic(1) title col fillstyle pattern 1 border ls 6 lw 1 dt 1, \\
		 '' using 7:xtic(1) title col fillstyle pattern 4 border ls 7 lw 1 dt 1
EOM
}

gnuplotcodetex () {
	# Write GNUplot code to file
	cat >$1 << EOM
#!/usr/bin/env gnuplot
set term cairolatex pdf color fontscale 0.44 size 6.5in,1in dashlength 0.2
set output '${2}'
set style data histogram
set style histogram cluster gap 1
set auto x
set key outside right center vertical Right samplen 2 width 4 spacing 1.2
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
		 '' using 4:xtic(1) title col fillstyle pattern 3 border ls 4 lw 1 dt 1, \\
		 '' using 5:xtic(1) title col fillstyle pattern 7 border ls 5 lw 1 dt 1, \\
		 '' using 6:xtic(1) title col fillstyle pattern 1 border ls 6 lw 1 dt 1, \\
		 '' using 7:xtic(1) title col fillstyle pattern 4 border ls 7 lw 1 dt 1
EOM
}

gnuplotlegend () {
	# Write GNUplot code to file
	cat >$1 << EOM
#!/usr/bin/env gnuplot
set term pdf enhanced color fontscale 0.44 size 5.7in,0.3in
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
		 '' using 4:xtic(1) fillstyle pattern 3 border ls 4 lw 1 dt 1 t "Early", \\
		 '' using 5:xtic(1) fillstyle pattern 7 border ls 5 lw 1 dt 1 t "Late", \\
		 '' using 6:xtic(1) fillstyle pattern 1 border ls 6 lw 1 dt 1 t "Continuous", \\
		 '' using 7:xtic(1) fillstyle pattern 4 border ls 7 lw 1 dt 1 t "Recoding"

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
set label 'Relative Runtime' at screen 0.5,0.15 rotate by 90
plot 1 ls 0 with linespoints
EOM
}




#################################
#################################
### Actual Script starts here ###
#################################
#################################

numcpus=$(nproc)
if [[ $? -ne 0 ]]; then
	numcpus=$(cat /proc/cpuinfo | grep processor | wc -l)
fi


# Compile
if [[ ${DO_COMPILE} -ne 0 ]]; then
	date
	echo "Compiling:"
	if [[ ${DO_COMPILE_CMAKE} -ne 0 ]] || [[ ! -e ${PATH_BUILD}/Makefile ]]; then
		echo " * Testing for compiler"
		cxx=$(which ${CXX_COMPILER})
		if [[ -e ${cxx} ]]; then
			export CXX=$cxx
		else
			cxx=$(which ${GXX_COMPILER})
			if [[ -e ${cxx} ]]; then
				export CXX=$cxx
			else
				echo "[ERROR] This benchmark requires a c++17 compatible compiler! You may modify variable CXX_COMPILER or GXX_COMPILER at the top of the script."
				exit 1
			fi
		fi
		testfile=$(mktemp --suffix=.cpp)
		cat >${testfile} << EOM
#include <optional>
int main() {
		return 0;
}
EOM
		${cxx} -std=c++17 -o ${testfile}.a ${testfile} &>/dev/null && (rm ${testfile}; echo "[INFO] Compiler is c++17 compatible.") || (rm ${estfile}; echo "[ERROR] Compiler is not c++17 compatible! Please fix this by yourself or tell me: Till.Kolditz@gmail.com."; exit 1)
		echo " * Recreating build dir \"${PATH_BUILD}\"."
		bootstrap_file="${PATH_BASE}/bootstrap.sh"
		if [[ -e "${bootstrap_file}" ]]; then
			echo "   * using bootstrap.sh file"
			pushd "${PATH_BASE}"
			${EXEC_ENV} ${EXEC_BASH} -c "${bootstrap_file}"
			popd
		else
			echo "   * bootstrap file not present -- trying to do it manually"
			rm -Rf ${PATH_BUILD}
			mkdir -p ${PATH_BUILD}
			pushd ${PATH_BUILD}
			echo " * Running cmake (Build Type = ${CMAKE_BUILD_TYPE}"
			cmake ${PATH_BASE} -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
			exitcode=$?
			popd
			if [[ ${exitcode} -ne 0 ]]; then
				echo " * [ERROR] Exitcode = ${exitcode}"
				exit ${exitcode};
			fi
		fi
	fi
	numcores=$(echo "${numcpus}*1.25/1"|bc)
	echo " * Running make (-j${numcores})"
	pushd ${PATH_BUILD}
	${EXEC_ENV} ${EXEC_BASH} -c "make -j${numcores}"
	exitcode=$?
	popd
	if [[ ${exitcode} -ne 0 ]]; then
		echo " * [ERROR] Exitcode = ${exitcode}"
		exit ${exitcode};
	fi
else
	echo "Skipping compilation."
fi

# Benchmarking
if [[ ${DO_BENCHMARK} -ne 0 ]]; then
	date
	( [[ "${BENCHMARK_MINBFW}" > 0 ]] && echo "Benchmarking (using AN-minBFW=${BENCHMARK_MINBFW}):" ) || echo "Benchmarking:"
	if (( AHEAD_USE_PCM==1 )); then
		echo -n " * checking for 'msr' module for performance counter monitoring: "
		(lsmod | grep msr &>/dev/null && echo "already loaded") || (sudo modprobe msr &>/dev/null && echo "loaded") || echo " could not load -- pcm counters not available"
	else
		echo " * Not using performance counter monitoring"
	fi

	if [[ ! -d ${PATH_EVALDATA} ]]; then
		mkdir -p ${PATH_EVALDATA}
	fi

	for ARCH in "${ARCHITECTURE[@]}"; do
		for NUM in "${IMPLEMENTED[@]}"; do
			BASE2=${BASE}${NUM}
			for VAR in "${VARIANTS[@]}"; do
				type="${BASE2}${VAR}${ARCH}"
				PATH_BINARY=${PATH_BUILD}/${type}
				echo -n " * ${type}:"
				if [[ -e ${PATH_BINARY} ]]; then
					EVAL_FILEOUT="${PATH_EVALDATA}/${type}.out"
					EVAL_FILEERR="${PATH_EVALDATA}/${type}.err"
					EVAL_FILETIME="${PATH_EVALDATA}/${type}.time"
					rm -f ${EVAL_FILEOUT} ${EVAL_FILEERR} ${EVAL_FILETIME}
					for SF in ${BENCHMARK_SCALEFACTORS[*]}; do
						echo -n " sf${SF}"
						echo "Scale Factor ${SF} ===========================" >>${EVAL_FILETIME}
						if [[ "${BENCHMARK_MINBFW}" > 0 ]]; then
							if (( AHEAD_USE_PCM==1 )); then
								sudo ${EXEC_ENV} ${EXEC_BASH} -c "/usr/bin/time -avo ${EVAL_FILETIME} ${PATH_BINARY} --numruns ${BENCHMARK_NUMRUNS} --verbose --print-result --dbpath \"${PATH_DB}/sf-${SF}${BENCHMARK_DBDIR_SUFFIX}\" --AN-minbfw ${BENCHMARK_MINBFW} 1>>${EVAL_FILEOUT} 2>>${EVAL_FILEERR}"
							else
								${EXEC_ENV} ${EXEC_BASH} -c "/usr/bin/time -avo ${EVAL_FILETIME} ${PATH_BINARY} --numruns ${BENCHMARK_NUMRUNS} --verbose --print-result --dbpath \"${PATH_DB}/sf-${SF}${BENCHMARK_DBDIR_SUFFIX}\" --AN-minbfw ${BENCHMARK_MINBFW} 1>>${EVAL_FILEOUT} 2>>${EVAL_FILEERR}"
							fi
						else
							if (( AHEAD_USE_PCM==1 )); then
								sudo ${EXEC_ENV} ${EXEC_BASH} -c "/usr/bin/time -avo ${EVAL_FILETIME} ${PATH_BINARY} --numruns ${BENCHMARK_NUMRUNS} --verbose --print-result --dbpath \"${PATH_DB}/sf-${SF}${BENCHMARK_DBDIR_SUFFIX}\" 1>>${EVAL_FILEOUT} 2>>${EVAL_FILEERR}"
							else
								sudo ${EXEC_ENV} ${EXEC_BASH} -c "/usr/bin/time -avo ${EVAL_FILETIME} ${PATH_BINARY} --numruns ${BENCHMARK_NUMRUNS} --verbose --print-result --dbpath \"${PATH_DB}/sf-${SF}${BENCHMARK_DBDIR_SUFFIX}\" 1>>${EVAL_FILEOUT} 2>>${EVAL_FILEERR}"
							fi
						fi
					done
					echo " done."
				else
					echo " Skipping missing binary '${${PATH_BINARY}}'!"
				fi
			done
		done
	done

	if (( AHEAD_USE_PCM==1 )); then
		USERID=$(id -u)
		GROUPID=$(id -g)
		for ARCH in "${ARCHITECTURE[@]}"; do
			for NUM in "${IMPLEMENTED[@]}"; do
				BASE2=${BASE}${NUM}
				for VAR in "${VARIANTS[@]}"; do
					type="${BASE2}${VAR}${ARCH}"
					PATH_BINARY=${PATH_BUILD}/${type}
					if [[ -e ${PATH_BINARY} ]]; then
						EVAL_FILEOUT="${PATH_EVALDATA}/${type}.out"
						EVAL_FILEERR="${PATH_EVALDATA}/${type}.err"
						sudo chown ${USERID}:${GROUPID} "${EVAL_FILEOUT}"
						sudo chown ${USERID}:${GROUPID} "${EVAL_FILEERR}"
					fi
				done
			done
		done
	fi
else
	echo "Skipping benchmarks."
fi

# Evaluation
if [[ ${DO_EVAL} -ne 0 ]]; then
	date
	echo "Evaluating"
	array=()

	[[ -d ${PATH_EVALDATA} ]] || mkdir -p ${PATH_EVALDATA}
	[[ -d ${PATH_EVALINTER} ]] || mkdir -p ${PATH_EVALINTER}
	[[ -d ${PATH_EVALREPORT} ]] || mkdir -p ${PATH_EVALREPORT}

	if [[ ${DO_EVAL_PREPARE} -ne 0 ]]; then
		echo " * Preparing"
	fi

	for ARCH in "${ARCHITECTURE[@]}"; do
		# Prepare File for a single complete normalized overhead graph across all scale factors
		EVAL_NORMALIZEDALLDATAFILE="norm-all${ARCH}.data"
		EVAL_NORMALIZEDALLDATAFILE_PATH="${PATH_EVALREPORT}/${EVAL_NORMALIZEDALLDATAFILE}"

		if [[ ${DO_EVAL_PREPARE} -ne 0 ]]; then
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
			EVAL_TEMPFILE_PATH="${PATH_EVALINTER}/${EVAL_TEMPFILE}"
			EVAL_DATAFILE="${BASE3}.data"
			EVAL_DATAFILE_PATH="${PATH_EVALREPORT}/${EVAL_DATAFILE}"
			EVAL_NORMALIZEDTEMPFILE="${BASE3}-norm.tmp"
			EVAL_NORMALIZEDTEMPFILE_PATH="${PATH_EVALINTER}/${EVAL_NORMALIZEDTEMPFILE}"
			EVAL_NORMALIZEDDATAFILE="${BASE3}-norm.data"
			EVAL_NORMALIZEDDATAFILE_PATH="${PATH_EVALREPORT}/${EVAL_NORMALIZEDDATAFILE}"

			if [[ ${DO_EVAL_PREPARE} -ne 0 ]]; then
				#njobs=$(jobs | wc -l)
				#((njobs/=136)) # you should adapt this one if the following subtask is changed
				#[[ $njobs -ge $(nproc) ]] && wait -n
				(
					echo "   * ${BASE3}"

					rm -f ${EVAL_TEMPFILE_PATH}
					rm -f ${EVAL_DATAFILE_PATH}
					echo -n "SF " >${EVAL_TEMPFILE_PATH}
					echo "${BENCHMARK_SCALEFACTORS[*]}" >>${EVAL_TEMPFILE_PATH}

					for VAR in "${VARIANTS[@]}"; do
						type="${BASE2}${VAR}${ARCH}"
						EVAL_FILEOUT="${PATH_EVALDATA}/${type}.out"
						EVAL_FILERESULTS_PATH="${PATH_EVALINTER}/${type}.results"
						EVAL_FILEBESTRUNS_PATH="${PATH_EVALINTER}/${type}.bestruns"

						grep -o 'result.*$' ${EVAL_FILEOUT} >${EVAL_FILERESULTS_PATH}
						allruntimes=($(grep -A ${BENCHMARK_NUMRUNS} "TotalTimes" ${EVAL_FILEOUT} | sed '/^--$/d' | grep -v "TotalTimes:" | awk '{print $2}'))

						rm -f ${EVAL_FILEBESTRUNS_PATH}
						echo -n "${type}" >>${EVAL_TEMPFILE_PATH}
						offset=0

						for sf in "${BENCHMARK_SCALEFACTORS[@]}"; do
							# a batch of ${BENCHMARK_NUMRUNS} runs, i.e. all runs of a scale factor
							# 1) compute the best runs (i.e. remove outliers)
							bestruns=($(printf "%s\n" "${allruntimes[@]:${offset}:${BENCHMARK_NUMRUNS}}" | sort -n | head -n ${BENCHMARK_NUMBEST}))
							echo "SF ${sf}: ${bestruns[@]}" >>${EVAL_FILEBESTRUNS_PATH}
							# 2) compute the arithmetic mean
							total=0
							for k in "${bestruns[@]}"; do
								((total += $k))
							done
							arithmean=$((total / ${BENCHMARK_NUMBEST}))
							# 3) append to file
							echo -n " ${arithmean}" >>${EVAL_TEMPFILE_PATH}
							((offset+=${BENCHMARK_NUMRUNS}))
						done
						echo "" >>${EVAL_TEMPFILE_PATH}
						sync ${EVAL_TEMPFILE_PATH}

						### process individual Operators' times
						EVAL_FILEOPS_TMP="${PATH_EVALINTER}/${type}.ops.tmp"
						EVAL_FILEOPS_OUT="${PATH_EVALINTER}/${type}.ops.out"
						grep op "${EVAL_FILEOUT}" | grep -v opy | sed -E 's/[ ]+//g' | sed -E 's/^\t//g' | sed -E 's/op([0-9]+\t)/\1/g' >"${EVAL_FILEOPS_TMP}"
						NUM_OPS_ARRAY=$(awk '
						BEGIN{i=0;mode=0;numCreateOps=0;numCopyOps=0;numQueryOps=0;FS="\t";}
						{
							if($1<i) {
								if(mode==0) {
									numCreateOps=i
									mode=1
								} else if (mode==1) {
									numCopyOps=i
									mode=2
								} else if (mode==2) {
									numQueryOps=i
									exit
								}
								i=$1
							} else {
								++i
							}
						}
						END{print numCreateOps,numCopyOps,numQueryOps}' "${EVAL_FILEOPS_TMP}")
						IFS=' ' read -r -a array <<< "${NUM_OPS_ARRAY}"
						NUM_OPS_CREATE=${array[0]}
						NUM_OPS_COPY=${array[1]}
						NUM_OPS_QUERY=${array[2]}
						awk -v numopscreate=${NUM_OPS_CREATE} -v numopscopy=${NUM_OPS_COPY} -v numopsquery=${NUM_OPS_QUERY} -v numruns=${BENCHMARK_NUMRUNS} '
							BEGIN{i=0;incopy=1;inquery=0;FS="\t";}
							{
								++i
								if ((incopy < 2) && (i == numopscreate)) {
								  ++incopy
								  i=0
								} else if ((incopy < 3) && (i == numopscopy)) {
								  ++incopy
								  i=0
								} else if (incopy == 3) {
								  print
								  if (i == numopsquery) {
									++inquery
									i=0
								  }
								  if (inquery == numruns) {
									incopy = 1
									inquery = 0
								  }
								}
							}
							END{printf "\n"}' "${EVAL_FILEOPS_TMP}" >"${EVAL_FILEOPS_OUT}"
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
					sync "${EVAL_TEMPFILE_PATH}" "${EVAL_DATAFILE_PATH}" "${EVAL_FILERESULTS_PATH}" "${EVAL_FILEBESTRUNS_PATH}" "${EVAL_NORMALIZEDDATAFILE_PATH}" "${EVAL_NORMALIZEDTEMPFILE_PATH}"
				) &
			fi ### [[ ${DO_EVAL_PREPARE} -ne 0 ]]
		done
	done
	# Now, wait for everything to finish
	wait -n
	sync

	if [[ ${DO_EVAL_PREPARE} -ne 0 ]]; then
		echo " * Preparing normalized data files for full-SSB plots"
		for ARCH in "${ARCHITECTURE[@]}"; do
			(
				# Prepare File for a single complete normalized overhead graph across all scale factors
				EVAL_NORMALIZEDALLDATAFILE="norm-all${ARCH}.data"
				EVAL_NORMALIZEDALLDATAFILE_PATH="${PATH_EVALREPORT}/${EVAL_NORMALIZEDALLDATAFILE}"
				for NUM in "${IMPLEMENTED[@]}"; do
					BASE2="${BASE}${NUM}"
					BASE3="${BASE2}${ARCH}"
					EVAL_TEMPFILE="${BASE3}.tmp"
					EVAL_TEMPFILE_PATH="${PATH_EVALINTER}/${EVAL_TEMPFILE}"
					EVAL_DATAFILE="${BASE3}.data"
					EVAL_DATAFILE_PATH="${PATH_EVALREPORT}/${EVAL_DATAFILE}"
					EVAL_NORMALIZEDTEMPFILE="${BASE3}-norm.tmp"
					EVAL_NORMALIZEDTEMPFILE_PATH="${PATH_EVALINTER}/${EVAL_NORMALIZEDTEMPFILE}"

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
				done
				sync -d ${EVAL_NORMALIZEDALLDATAFILE_PATH}
			) &
		done
	fi ### [[ ${DO_EVAL_PREPARE} -ne 0 ]]
	# Now, wait for everything to finish
	wait -n
	sync

	echo " * Collecting Data for teaser graphs"
	EVAL_TEASER_STORAGEFILE="teaser.storage.data"
	EVAL_TEASER_STORAGEFILE_PATH="${PATH_EVALREPORT}/${EVAL_TEASER_STORAGEFILE}"
	EVAL_TEASER_RUNTIMEFILE="teaser.runtime.data"
	EVAL_TEASER_RUNTIMEFILE_PATH="${PATH_EVALREPORT}/${EVAL_TEASER_RUNTIMEFILE}"
	rm -f ${EVAL_TEASER_STORAGEFILE_PATH} ${EVAL_TEASER_RUNTIMEFILE}

	# The storage teaser graph is for the INTERMEDIATE RESULTS only!
	# For the storage teaser graph, we only use scale factor 1 AND WE ASSUME THAT THIS IS THE FIRST ONE IN THE RESULT FILES!
	echo "   * Storage"
	totalAbsoluteStorages=()
	for VAR in "${VARIANTS[@]}"; do
		totalAbsoluteStorages+=(0)
	done
	for ARCH in "${ARCHITECTURE[0]}"; do
		for NUM in "${IMPLEMENTED[@]}"; do
			printf '%s' ${NUM} >>"${EVAL_TEASER_STORAGEFILE_PATH}"
			BASE2="${BASE}${NUM}"
			# In the following, we first sum up the storage consumption (per query ${NUM}) and then divide the totals
			for idx in "${!VARIANTS[@]}"; do
				type="${BASE2}${VARIANTS[$idx]}${ARCH}"
				EVAL_FILEOPS_OUT="${PATH_EVALINTER}/${type}.ops.out"
				# use the projected consumption for the teaser
				STORAGE=$(awk '
					BEGIN {opNumBefore=0; projected=0}
					opNumBefore < $1 {
						opNumBefore=$1
						projected+=$5
						next
					}
					{exit}
					END {printf "\t%s", projected}
				' "${EVAL_FILEOPS_OUT}")
				((totalAbsoluteStorages[idx] += STORAGE))
			done
			printf '\n' >>"${EVAL_TEASER_STORAGEFILE_PATH}"
		done
	done
	printf 'Type' >"${EVAL_TEASER_STORAGEFILE_PATH}"
	for idx in "${!VARIANTS[@]}"; do
		printf '\t%s' "${VARIANT_NAMES[$idx]}" >>"${EVAL_TEASER_STORAGEFILE_PATH}"
	done
	printf '\nAverage' >>"${EVAL_TEASER_STORAGEFILE_PATH}"
	for idx in ${!VARIANTS[@]}; do
		overallAverage=$(echo "${totalAbsoluteStorages[$idx]} ${totalAbsoluteStorages[0]}"|awk '{printf "%f", $1 / $2}')
		printf "\t${overallAverage}" >>"${EVAL_TEASER_STORAGEFILE_PATH}"
	done

	echo "   * Runtime"
	totalRelativeRuntimes=()
	for VAR in "${VARIANTS[@]}"; do
		totalRelativeRuntimes+=(0)
	done
	for idx in "${!ARCHITECTURE[@]}"; do
		EVAL_NORMALIZEDALLDATAFILE="norm-all${ARCHITECTURE[$idx]}.data"
		EVAL_NORMALIZEDALLDATAFILE_PATH="${PATH_EVALREPORT}/${EVAL_NORMALIZEDALLDATAFILE}"
		# ignore the headline generated in the normalized data file
		#echo -n "      * ${ARCHITECTURE_NAMES[$idx]}"
		ARRAY_RUNTIMES=($(awk -v numvars=${#VARIANTS[@]} '
			BEGIN {for (i=0; i<numvars; ++i) runtimes[i]=0}
			NR>1{for (i=1; i<NF; ++i) runtimes[i-1]+=$i}
			END{for (i in runtimes) print runtimes[i]}
		' "${EVAL_NORMALIZEDALLDATAFILE_PATH}"))
		for idx in "${!VARIANTS[@]}"; do
			totalRelativeRuntimes[$idx]=$(echo "${totalRelativeRuntimes[$idx]}+${ARRAY_RUNTIMES[$idx]}"|bc)
		done
	done
	#echo -n " [${totalRelativeRuntimes[@]}]"
	numRuntimes=$((${#ARCH[@]}*${#IMPLEMENTED[@]}))
	printf 'Type' >"${EVAL_TEASER_RUNTIMEFILE_PATH}"
	for idx in "${!VARIANTS[@]}"; do
		printf '\t%s' "${VARIANT_NAMES[$idx]}" >>"${EVAL_TEASER_RUNTIMEFILE_PATH}"
	done
	printf '\nAverage' >>"${EVAL_TEASER_RUNTIMEFILE_PATH}"
	#echo -n " ["
	for idx in "${!VARIANTS[@]}"; do
		#[[ $idx > 0 ]] && echo -n " "
		overallAverage=$(echo "${totalRelativeRuntimes[$idx]} ${numRuntimes}"|awk '{printf "%f", $1 / $2}')
		#echo -n "${overallAverage}"
		printf '\t%s' "${overallAverage}" >>"${EVAL_TEASER_RUNTIMEFILE_PATH}"
	done
	#echo "]"

	echo " * Plotting"
	for ARCH in "${ARCHITECTURE[@]}"; do
		# Prepare File for a single complete normalized overhead graph across all scale factors
		EVAL_NORMALIZEDALLDATAFILE="norm-all${ARCH}.data"
		EVAL_NORMALIZEDALLDATAFILE_PATH="${PATH_EVALREPORT}/${EVAL_NORMALIZEDALLDATAFILE}"
		EVAL_NORMALIZEDALLPLOTFILE="norm-all${ARCH}.m"
		EVAL_NORMALIZEDALLPLOTFILE_PATH="${PATH_EVALREPORT}/${EVAL_NORMALIZEDALLPLOTFILE}"
		EVAL_NORMALIZEDALLTEXFILE="norm-all${ARCH}.tex"
		EVAL_NORMALIZEDALLTEXFILE_PATH="${PATH_EVALREPORT}/${EVAL_NORMALIZEDALLTEXFILE}"
		for NUM in "${IMPLEMENTED[@]}"; do
			BASE2="${BASE}${NUM}"
			BASE3="${BASE2}${ARCH}"
			EVAL_TEMPFILE="${BASE3}.tmp"
			EVAL_TEMPFILE_PATH="${PATH_EVALINTER}/${EVAL_TEMPFILE}"
			EVAL_DATAFILE="${BASE3}.data"
			EVAL_DATAFILE_PATH="${PATH_EVALREPORT}/${EVAL_DATAFILE}"
			EVAL_NORMALIZEDTEMPFILE="${BASE3}-norm.tmp"
			EVAL_NORMALIZEDTEMPFILE_PATH="${PATH_EVALINTER}/${EVAL_NORMALIZEDTEMPFILE}"
			EVAL_NORMALIZEDDATAFILE="${BASE3}-norm.data"
			EVAL_NORMALIZEDDATAFILE_PATH="${PATH_EVALREPORT}/${EVAL_NORMALIZEDDATAFILE}"
			echo "   * ${BASE3}"
			pushd ${PATH_EVALREPORT}
			#gnuplotcode <output file> <gnuplot target output file> <gnuplot data file>
			gnuplotcodepdf ${BASE3}.m ${BASE3}.pdf ${EVAL_DATAFILE} \
				"set yrange [0:*]" "set grid" "set xlabel 'Scale Factor'" "set ylabel 'Runtime [ns]'"
			gnuplotcodepdf ${BASE3}-norm.m ${BASE3}-norm.pdf ${EVAL_NORMALIZEDDATAFILE} \
				"set yrange [0.9:2]" "set ytics out" "set xtics out" "set grid noxtics ytics" "unset xlabel" "unset ylabel"
			gnuplotlegend ${BASE3}-legend.m ${BASE3}-legend.pdf ${BASE3}-ylabel.pdf ${BASE3}.data
			gnuplot ${BASE3}.m
			gnuplot ${BASE3}-norm.m
			gnuplot ${BASE3}-legend.m
			popd
		done
		sync

		#ALLPDFOUTFILE=${PATH_EVALREPORT}/ssbm-all${ARCH}.pdf
		#ALLPDFINFILES=
		#for NUM in "${IMPLEMENTED[@]}"; do
		#	BASE3=${BASE}${NUM}${ARCH}
		#	ALLPDFINFILES+=" ${PATH_EVALREPORT}/${BASE3}.pdf ${PATH_EVALREPORT}/${BASE3}-norm.pdf"
		#done
		#echo "   * Creating PDF file with all diagrams (${ALLPDFOUTFILE})"
		#gs -sDEVICE=pdfwrite -dCompatibilityLevel=1.4 -dPDFSETTINGS=/default -dNOPAUSE -dQUIET -dBATCH -dDetectDuplicateImages -dCompressFonts=true -r150 -sOutputFile=${ALLPDFOUTFILE} ${ALLPDFINFILES}

		echo "   * Creating tex/PDF files for normalized averages (${EVAL_NORMALIZEDALLTEXFILE_PATH})"
		gnuplotcodetex  ${EVAL_NORMALIZEDALLPLOTFILE_PATH} ${EVAL_NORMALIZEDALLTEXFILE_PATH} ${EVAL_NORMALIZEDALLDATAFILE_PATH} \
			"set yrange [0:]" "set ytics out" "set xtics out" "set grid noxtics ytics" "unset xlabel" "set ylabel \"Relative Runtime\""
		gnuplot ${EVAL_NORMALIZEDALLPLOTFILE_PATH}
	done
	echo "   * Teaser graphs"
	sync
else
	echo "Skipping evaluation."
fi

# Verification
if [[ ${DO_VERIFY} -ne 0 ]]; then
	date
	echo "Verifying:"
	NUMARCHS="${#ARCHITECTURE[@]}"
	isAnyBad=0
	for idxArch in $(seq 0 $(echo "${NUMARCHS}-1" | bc)); do
		ARCH=${ARCHITECTURE[$idxArch]}
		ARCHNAME=${ARCHITECTURE_NAMES[$idxArch]}
		echo " * ${ARCHNAME}"
		for NUM in "${IMPLEMENTED[@]}"; do
			echo -n "   * Q${NUM}:"
			BASE2="${BASE}${NUM}"
			NUMVARS="${#VARIANTS[@]}"
			baseline1="${PATH_EVALINTER}/${BASE2}${VARIANTS[0]}${ARCHITECTURE[0]}.results"
			baseline1Tmp="${baseline1}.tmp"
			baseline2="${PATH_EVALDATA}/${BASE2}${VARIANTS[0]}${ARCH}.err"
			awk '{print $1,$2}' "${baseline1}" >"${baseline1Tmp}"
			for VAR in "${VARIANTS[@]}"; do
				echo -n " ${VAR:1}="
				other1="${PATH_EVALINTER}/${BASE2}${VAR}${ARCH}.results"
				other1Tmp="${other1}.tmp"
				awk '{print $1,$2}' "${other1}" >"${other1Tmp}" # filters out e.g. the encoded value for the continuous encoding variants
				other2="${PATH_EVALDATA}/${BASE2}${VAR}${ARCH}.err"
				RES1=$(diff "${baseline1Tmp}" "${other1Tmp}" | wc -l)
				RES2=$(diff "${baseline2}" "${other2}" | wc -l)
				if [[ "${RES1}" -eq 0 ]] && [[ "${RES2}" -eq 0 ]]; then
					echo -n "OK";
				else
					echo -n "BAD";
					isAnyBad=1
					if [[ "${RES2}" -eq 0 ]]; then echo -n "(result)"; else echo -n "(err)"; fi
				fi
			done
			echo ""
		done
	done

	if ((isAnyBad == 1)); then
		echo "Generating diffs:"
		for s in "${ARCHITECTURE[@]}"; do
			for q in "${IMPLEMENTED[@]}"; do
				for v in "${VARIANTS[@]}"; do
					for t in out err; do
						grepfile="./grep.${t}"
						diff "${PATH_EVALDATA}/${BASE}${q}_normal_scalar.${t}" "${PATH_EVALDATA}/${BASE}${q}${v}${s}.${t}" | grep result >${grepfile}
						if [[ -s "${grepfile}" ]]; then
							echo "Q${q}${v}${s} (${t}):"
							cat "${grepfile}"
							echo "-------------------------------"
						fi
						rm -f ${grepfile}
					done
				done
			done
		done
	fi
else
	echo "Skipping verification."
fi
