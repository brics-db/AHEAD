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
				AHEAD_DATE="${ARGS[1]}" #only needed when calling the original script.
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
				AHEAD_DATE="${ARGS[1]}" #only needed when calling the original script.
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
CONFIG_FILE=run.conf
source "${CONFIG_FILE}" || exit 1
echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Used date: ${AHEAD_DATE}"
export AHEAD_DATE

mkdir -p "${PATH_EVAL_CURRENT}" || AHEAD_quit 1 "${AHEAD_SCRIPT_COMMAND_PREFIX}Could not create path '${PATH_EVAL_CURRENT}'"

################################################################################################
# if the outputs are not redirected to files, then call ourselves again with additional piping #
################################################################################################
if [[ -t 1 ]] && [[ -t 2 ]]; then
	filebase=$(basename -s '.sh' $0)
	outfile="${PATH_EVAL_CURRENT}/${filebase}.out"
	errfile="${PATH_EVAL_CURRENT}/${filebase}.err"
	if [[ -e "${PATH_EVAL_CURRENT}" ]]; then
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
	echo "${AHEAD_SCRIPT_COMMAND_PREFIX}[INFO] one of stdout or stderr is not redirected, calling script with redirecting" | tee $outfile
	./$0 $@ > >(tee "${outfile}") 2> >(tee "${errfile}" >&2)
	ret=$?
	if [[ ${ret} != 0 ]]; then
		echo "${AHEAD_SCRIPT_COMMAND_PREFIX}[ERROR] Aborted. Exit code = $ret" > >(tee -a $errfile >&2)
	else
		echo "${AHEAD_SCRIPT_COMMAND_PREFIX}[INFO] Finished" > >(tee -a $outfile)
	fi
	exit $ret
else
	echo "${AHEAD_SCRIPT_COMMAND_PREFIX}[INFO] stdout and stderr are redirected. Starting script. Parameters are: \"$@\""
fi ### [[ -t 1 ]] && [[ -t 2 ]]

# copy the script file to the sub-eval-folder and disable / change some lines to only enable data evaluation at a later time (e.g. to adapt the gnuplot scripts)
sed -E -e '34,+25s/^(.+)$/#\1/' -e '88s/^.+$/\tPHASE="EVAL"\n\tDO_COMPILE=0\n\tDO_BENCHMARK=0\n\tDO_EVAL=1\n\tDO_EVAL_PREPARE=1\n\tDO_VERIFY=1/' -e '94s/^(.+)$/export AHEAD_DATE="'"${AHEAD_DATE}"'"\nexport PATH_EVAL_CURRENT="."\n\1/' -e '134,+2s/^(.+)$/#\1/' $0 >"${PATH_EVAL_CURRENT}/$(basename $0)"
sed -E -e 's/(source\s+"[^\.]+)(\.\.\/common.conf.+)$/\1..\/\2/' "${CONFIG_FILE}" >"${PATH_EVAL_CURRENT}/${CONFIG_FILE}"
chmod +x "${PATH_EVAL_CURRENT}/$(basename $0)"

echo "${AHEAD_SCRIPT_COMMAND_PREFIX}[INFO] Running Phase \"${PHASE}\""

###################
# Basic Variables #
###################
[[ -z "${CXX_COMPILER+x}" ]] && CXX_COMPILER="c++"
[[ -z "${GXX_COMPILER+x}" ]] && GXX_COMPILER="g++"
BASE=ssbm-q
BASEREPLACE1="s/${BASE}\([0-9]\)\([0-9]\)/Q\1.\2/g"
BASEREPLACE2="s/[_]\([^[:space:]]\)[^[:space:]]*/^\{\1\}/g"
IMPLEMENTED=(11 12 13 21 22 23 31 32 33 34 41 42 43)
# ATTENTION !!! When you change the following, then you MUST adapt variable TEASER_INDICES, too !!!
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
TEASER_INDICES=(0 1 4)

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
[ -z ${BENCHMARK_DBDIR_SUFFIX+x} ] && BENCHMARK_DBDIR_SUFFIX=
[ -z ${BENCHMARK_MINBFW+x} ] && BENCHMARK_MINBFW=

### Eval
EVAL_TOTALRUNS_PER_VARIANT=$(echo "$BENCHMARK_NUMRUNS * ${#BENCHMARK_SCALEFACTORS[@]}"|bc)

#################
# functions etc #
#################

date () {
	echo -n "${AHEAD_SCRIPT_COMMAND_PREFIX}Current timestamp: "; command date "+%Y-%m-%d %H-%M-%S"
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

gnuplot_teaser_runtime () {
	cat >$1 << EOM
#!/usr/bin/env gnuplot

set datafile separator '\t'
set style data histogram
set style histogram cluster gap 0
set style fill pattern 0 border
set style line 1 lc rgb "#9400d3"
set style line 2 lc rgb "#009e73"
set style line 3 lc rgb "#56b4e9"
set style line 4 lc rgb "#e69f00"
set style line 5 lc rgb "#000000"
set style line 6 lc rgb "#0072b2"
set style line 7 lc rgb "#e51e10"

set term cairolatex pdf color fontscale 0.44 size 1in,1in dashlength 0.2
set output '${2}'
unset key
set boxwidth 0.75
set auto x
unset xtics
set yrange [0:2.5]
set grid noxtics noytics
set ylabel "Relative Runtime" offset 1,0
unset xlabel
set tmargin 0.5
set rmargin 0.5
set bmargin 1
set lmargin 8
set format y "\\\\num{%g}"
set label "${4}" at first -0.5,${5}
set label "${6}" at first -0.05,${7}
set label "${8}" at first 0.4,${9}
plot '${3}' using 2:xtic(1) fillstyle pattern 0 border ls 1 lw 1 dt 1 title col, \\
         '' using 3:xtic(1) fillstyle pattern 2 border ls 2 lw 1 dt 1 title col, \\
         '' using 4:xtic(1) fillstyle pattern 1 border ls 6 lw 1 dt 1 title col
unset output
EOM
}

gnuplot_teaser_consumption () {
	cat >$1 << EOM
#!/usr/bin/env gnuplot

set datafile separator '\t'
set style data histogram
set style histogram cluster gap 0
set style fill pattern 0 border
set style line 1 lc rgb "#9400d3"
set style line 2 lc rgb "#009e73"
set style line 3 lc rgb "#56b4e9"
set style line 4 lc rgb "#e69f00"
set style line 5 lc rgb "#000000"
set style line 6 lc rgb "#0072b2"
set style line 7 lc rgb "#e51e10"

set term cairolatex pdf color fontscale 0.44 size 1in,1in dashlength 0.2
set output '${2}'
unset key
set boxwidth 0.75
set auto x
unset xtics
set yrange [0:2.5]
set grid noxtics noytics
set ylabel "Relative Memory\nConsumption" offset 1,0
unset xlabel
set tmargin 0.5
set rmargin 0.5
set bmargin 1
set lmargin 8
set format y "\\\\num{%g}"
set label "${4}" at first -0.5,${5}
set label "${6}" at first -0.05,${7}
set label "${8}" at first 0.4,${9}
plot '${3}' using 2:xtic(1) fillstyle pattern 0 border ls 1 lw 1 dt 1 title col, \\
         '' using 3:xtic(1) fillstyle pattern 2 border ls 2 lw 1 dt 1 title col, \\
         '' using 4:xtic(1) fillstyle pattern 1 border ls 6 lw 1 dt 1 title col
unset output
EOM
}

gnuplot_teaser_legend () {
	cat >$1 << EOM
#!/usr/bin/env gnuplot

set datafile separator '\t'
set style data histogram
set style histogram cluster gap 0
set style fill pattern 0 border
set style line 1 lc rgb "#9400d3"
set style line 2 lc rgb "#009e73"
set style line 3 lc rgb "#56b4e9"
set style line 4 lc rgb "#e69f00"
set style line 5 lc rgb "#000000"
set style line 6 lc rgb "#0072b2"
set style line 7 lc rgb "#e51e10"

set term cairolatex pdf color fontscale 0.44 size 0.8in,1in
set output '${2}'
unset label
unset grid
unset box
unset border
set margin 0
set key right outside center spacing 3
unset tics
unset xlabel
unset ylabel
set yrange [-10:0]
plot '${3}' using 2:xtic(1) fillstyle pattern 0 border ls 1 lw 1 dt 1 t "Unprotected", \\
         '' using 3:xtic(1) fillstyle pattern 2 border ls 2 lw 1 dt 1 t "DMR", \\
         '' using 4:xtic(1) fillstyle pattern 1 border ls 6 lw 1 dt 1 t "AHEAD"
unset output
EOM
}

gnuplot_scalarVSvector () {
	cat >$1 << EOM
#!/usr/bin/env gnuplot

set datafile separator '\t'

set style line 1 lc rgb "#9400d3"
set style line 2 lc rgb "#009e73"
set style line 3 lc rgb "#56b4e9"
set style line 4 lc rgb "#e69f00"
set style line 5 lc rgb "#000000"
set style line 6 lc rgb "#0072b2"
set style line 7 lc rgb "#e51e10"

set term cairolatex pdf color fontscale 0.44 size 2.9in,1in dashlength 0.2
set output '${2}'
set style data histogram
set style histogram cluster gap 1
set auto x
set key below horizontal
set ytics mirror out
set xtics nomirror out
set grid noxtics ytics
unset xlabel
set ylabel "Relative Runtime"
$(for var in "${@:4}"; do echo $var; done)
plot '${3}' using 2:xtic(1) title col fillstyle pattern 0 border ls 1 lw 1 dt 1, \
         '' using 3:xtic(1) title col fillstyle pattern 2 border ls 2 lw 1 dt 1
unset output
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
if ((DO_COMPILE != 0)); then
	echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Compiling."
	AHEAD_sub_begin
	date
	if ((DO_COMPILE_CMAKE != 0)) || [[ ! -e ${PATH_BUILD}/Makefile ]]; then
		echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Testing for compiler."
		cxx=$(which ${CXX_COMPILER})
		if [[ -e ${cxx} ]]; then
			export CXX=$cxx
		else
			cxx=$(which ${GXX_COMPILER})
			if [[ -e ${cxx} ]]; then
				export CXX=$cxx
			else
				AHEAD_quit 1 "${AHEAD_SCRIPT_COMMAND_PREFIX}[ERROR] This benchmark requires a c++17 compatible compiler! You may modify variable CXX_COMPILER or GXX_COMPILER at the top of the script."
			fi
		fi
		testfile=$(mktemp --suffix=.cpp) || exit 1
		cat >${testfile} << EOM
#include <optional>
int main() {
		return 0;
}
EOM
		${cxx} -std=c++17 -o ${testfile}.a ${testfile} &>/dev/null && (rm ${testfile}; echo "${AHEAD_SCRIPT_COMMAND_PREFIX}[INFO] Compiler is c++17 compatible.") || (rm ${estfile}; echo "${AHEAD_SCRIPT_COMMAND_PREFIX}[ERROR] Compiler is not c++17 compatible! Please fix this by yourself or tell me: Till.Kolditz@gmail.com."; exit 1)
		echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Recreating build dir \"${PATH_BUILD}\"."
		if [[ -e "${AHEAD_SCRIPT_BOOTSTRAP}" ]]; then
			echo -n "${AHEAD_SCRIPT_COMMAND_PREFIX}using bootstrap.sh file..."
			AHEAD_pushd "${PATH_BASE}"
			AHEAD_run_hidden_output "${AHEAD_SCRIPT_BOOTSTRAP}" || exit 1
			AHEAD_popd
		else
			echo "${AHEAD_SCRIPT_COMMAND_PREFIX}bootstrap file not present -- trying to do it manually"
			rm -Rf ${PATH_BUILD}
			mkdir -p ${PATH_BUILD}
			AHEAD_pushd ${PATH_BUILD}
			echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Running cmake (Build Type = ${CMAKE_BUILD_TYPE}..."
			AHEAD_run_hidden_output cmake "${PATH_BASE}" "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
			exitcode=$?
			AHEAD_popd
			if [[ ${exitcode} -ne 0 ]]; then
				echo "${AHEAD_SCRIPT_COMMAND_PREFIX}[ERROR] Exitcode = ${exitcode}"
				exit ${exitcode};
			fi
		fi
	fi
	numcores=$(echo "${numcpus}*1.25/1"|bc)
	AHEAD_pushd ${PATH_BUILD}
	echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Running make (-j${numcores})..."
	AHEAD_run_hidden_output "make" "-j${numcores}" || exit 1
	AHEAD_popd
	AHEAD_sub_end
else
	echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Skipping compilation."
fi

# Benchmarking
if ((DO_BENCHMARK != 0)); then
	echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Benchmarking."
	AHEAD_sub_begin
	date
	AHEAD_prepare_scalinggovernor_and_turboboost
	((BENCHMARK_MINBFW > 0 )) && echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Using AN-minBFW=${BENCHMARK_MINBFW})."
	if (( AHEAD_USE_PCM==1 )); then
		echo -n "${AHEAD_SCRIPT_COMMAND_PREFIX}checking for 'msr' module for performance counter monitoring: "
		(lsmod | grep msr &>/dev/null && echo "already loaded") || (sudo modprobe msr &>/dev/null && echo "loaded") || echo " could not load -- pcm counters not available"
	else
		echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Not using performance counter monitoring"
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
				echo -n "${AHEAD_SCRIPT_COMMAND_PREFIX}${type}:"
				if [[ -e ${PATH_BINARY} ]]; then
					EVAL_FILEOUT="${PATH_EVALDATA}/${type}.out"
					EVAL_FILEERR="${PATH_EVALDATA}/${type}.err"
					EVAL_FILETIME="${PATH_EVALDATA}/${type}.time"
					rm -f ${EVAL_FILEOUT} ${EVAL_FILEERR} ${EVAL_FILETIME}
					for SF in ${BENCHMARK_SCALEFACTORS[*]}; do
						echo -n " sf-${SF}"
						echo "Scale Factor ${SF} ===========================" >>${EVAL_FILETIME}
						if (( BENCHMARK_MINBFW > 0 )); then
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
		echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Fixing file permissions"
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
	AHEAD_sub_end
else
	echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Skipping benchmarks."
fi

# Evaluation
if ((DO_EVAL != 0)); then
	echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Evaluating."
	AHEAD_sub_begin
	date
	array=()

	[[ -d ${PATH_EVALDATA} ]] || mkdir -p ${PATH_EVALDATA}
	[[ -d ${PATH_EVALINTER} ]] || mkdir -p ${PATH_EVALINTER}
	[[ -d ${PATH_EVALREPORT} ]] || mkdir -p ${PATH_EVALREPORT}

	if ((DO_EVAL_PREPARE != 0)); then
		echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Preparing"
		AHEAD_sub_begin
		for ARCH in "${ARCHITECTURE[@]}"; do
			# Prepare File for a single complete normalized overhead graph across all scale factors
			EVAL_NORMALIZEDALLDATAFILE="norm-all${ARCH}.data"
			EVAL_NORMALIZEDALLDATAFILE_PATH="${PATH_EVALREPORT}/${EVAL_NORMALIZEDALLDATAFILE}"

			rm -f ${EVAL_NORMALIZEDALLDATAFILE_PATH}
			echo -n "Query" >>${EVAL_NORMALIZEDALLDATAFILE_PATH}
			for var in "${VARIANT_NAMES[@]}"; do
				echo -n " ${var}" >>${EVAL_NORMALIZEDALLDATAFILE_PATH}
			done
			echo "" >>${EVAL_NORMALIZEDALLDATAFILE_PATH}

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

				(
					echo "${AHEAD_SCRIPT_COMMAND_PREFIX}${BASE3}"

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
						NUM_OPS_ARRAY=$(awk \
						'BEGIN{i=0;mode=0;numCreateOps=0;numCopyOps=0;numQueryOps=0;FS="\t";}
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
						awk -v numopscreate=${NUM_OPS_CREATE} -v numopscopy=${NUM_OPS_COPY} -v numopsquery=${NUM_OPS_QUERY} -v numruns=${BENCHMARK_NUMRUNS} \
							'BEGIN{i=0;incopy=1;inquery=0;FS="\t";}
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
					awk "${arg}" "${EVAL_TEMPFILE_PATH}" "${EVAL_TEMPFILE_PATH}" >"${EVAL_NORMALIZEDTEMPFILE_PATH}"

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
			done
		done
		# Now, wait for everything to finish
		wait -n
		AHEAD_sync
		AHEAD_sub_end

		echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Preparing normalized data files for full-SSB plots"
		AHEAD_sub_begin
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
					awk "${arg}" "${EVAL_NORMALIZEDTEMPFILE_PATH}" "${EVAL_NORMALIZEDTEMPFILE_PATH}" >>"${EVAL_NORMALIZEDALLDATAFILE_PATH}"
					echo "" >>"${EVAL_NORMALIZEDALLDATAFILE_PATH}"
					# remove <zero>-bytes
					tr <"${EVAL_NORMALIZEDALLDATAFILE_PATH}" -d '\000' >"${EVAL_NORMALIZEDALLDATAFILE_PATH}.tmp"
					mv "${EVAL_NORMALIZEDALLDATAFILE_PATH}.tmp" "${EVAL_NORMALIZEDALLDATAFILE_PATH}"
				done
				sync -d ${EVAL_NORMALIZEDALLDATAFILE_PATH}
			) &
		done
		# Now, wait for everything to finish
		wait -n
		AHEAD_sync
		AHEAD_sub_end
	else ### ((DO_EVAL_PREPARE != 0))
		echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Skipping preparation."
	fi ### ((DO_EVAL_PREPARE != 0))

	echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Collecting Data for teaser graphs"
	AHEAD_sub_begin
	rm -f ${PATH_TEASER_CONSUMPTION_DATAFILE} ${PATH_TEASER_RUNTIME_DATAFILE}

	# The storage teaser graph is for the INTERMEDIATE RESULTS only!
	# For the storage teaser graph, we only use scale factor 1 AND WE ASSUME THAT THIS IS THE FIRST ONE IN THE RESULT FILES!
	echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Memory Consumption"
	totalAbsoluteStorages=()
	for VAR in "${VARIANTS[@]}"; do
		totalAbsoluteStorages+=(0)
	done
	for ARCH in "${ARCHITECTURE[0]}"; do
		for NUM in "${IMPLEMENTED[@]}"; do
			printf '%s' ${NUM} >>"${PATH_TEASER_CONSUMPTION_DATAFILE}"
			BASE2="${BASE}${NUM}"
			# In the following, we first sum up the storage consumption (per query ${NUM}) and then divide the totals
			# We assume that file ${EVAL_FILEOPS_OUT} contains only the filtered operator stats AND
			# that the first N entries belong to the very first query. We collect all consumptions until the operator number is smaller again, which means the next query run starts, and then stop awk.
			for idx in "${TEASER_INDICES[@]}"; do
				type="${BASE2}${VARIANTS[$idx]}${ARCH}"
				EVAL_FILEOPS_OUT="${PATH_EVALINTER}/${type}.ops.out"
				# use the projected consumption for the teaser
				STORAGE=$(awk \
					'BEGIN {opNumBefore=0; projected=0}
					opNumBefore < $1 {
						opNumBefore=$1
						projected+=$5
						next
					}
					{exit}
					END {printf "\t%s", projected}' \
					"${EVAL_FILEOPS_OUT}")
				((totalAbsoluteStorages[idx] += STORAGE))
			done
			printf '\n' >>"${PATH_TEASER_CONSUMPTION_DATAFILE}"
		done
	done
	printf 'Type' >"${PATH_TEASER_CONSUMPTION_DATAFILE}"
	for idx in "${TEASER_INDICES[@]}"; do
		printf '\t%s' "${VARIANT_NAMES[$idx]}" >>"${PATH_TEASER_CONSUMPTION_DATAFILE}"
	done
	printf '\nAverage' >>"${PATH_TEASER_CONSUMPTION_DATAFILE}"
	CONSUMPTION_ARGUMENTS=()
	for idx in ${TEASER_INDICES[@]}; do
		overallAverage=$(awk '{printf "%f", $1 / $2}' <<<"${totalAbsoluteStorages[$idx]} ${totalAbsoluteStorages[0]}")
		printf "\t${overallAverage}" >>"${PATH_TEASER_CONSUMPTION_DATAFILE}"
		CONSUMPTION_ARGUMENTS+=($(awk '{printf "%.2f %.2f",$1,($1+0.25)}' <<<"${overallAverage}"))
	done

	echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Runtime"
	AHEAD_sub_begin
	totalRelativeRuntimes=()
	for VAR in "${VARIANTS[@]}"; do
		totalRelativeRuntimes+=(0)
	done
	# For the following, we can use the already computed overall-relative-runtimes! These are stored in file ${EVAL_NORMALIZEDALLDATAFILE_PATH}
	numRuntimes=$((${#ARCHITECTURE[@]}*${#IMPLEMENTED[@]}))
	if [[ ! -z ${VERBOSE+x} ]]; then
		echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Architectures: ${ARCHITECTURE[@]}"
		echo "${AHEAD_SCRIPT_COMMAND_PREFIX}numRuntimes=${numRuntimes}"
	fi
	for idx in "${!ARCHITECTURE[@]}"; do
		[[ ! -z ${VERBOSE+x} ]] && echo "${AHEAD_SCRIPT_COMMAND_PREFIX}${ARCHITECTURE_NAMES[$idx]}"
		AHEAD_sub_begin
		EVAL_NORMALIZEDALLDATAFILE="norm-all${ARCHITECTURE[$idx]}.data"
		EVAL_NORMALIZEDALLDATAFILE_PATH="${PATH_EVALREPORT}/${EVAL_NORMALIZEDALLDATAFILE}"
		[[ ! -z ${VERBOSE+x} ]] && echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Normalized data file: ${EVAL_NORMALIZEDALLDATAFILE_PATH}"
		# ignore the headline generated in the normalized data file
		ARRAY_RUNTIMES=($(awk -v numvars=${#VARIANTS[@]} \
			'BEGIN{for (i=0; i<numvars; ++i) runtimes[i]=0}
			NR>1{for (i=1; i<=NF; ++i) runtimes[i-1]+=$i}
			END{for (i=1; i<=numvars; ++i) print runtimes[i]}' \
			"${EVAL_NORMALIZEDALLDATAFILE_PATH}"))
		for idxA in "${!VARIANTS[@]}"; do
			totalRelativeRuntimes[$idxA]=$(echo "${totalRelativeRuntimes[$idxA]}+${ARRAY_RUNTIMES[$idxA]}"|bc)
		done
		[[ ! -z ${VERBOSE+x} ]] && echo "${AHEAD_SCRIPT_COMMAND_PREFIX}[${totalRelativeRuntimes[@]}]"
		AHEAD_sub_end
	done
	printf 'Type' >>"${PATH_TEASER_RUNTIME_DATAFILE}"
	for idx in "${TEASER_INDICES[@]}"; do
		printf '\t%s' "${VARIANT_NAMES[$idx]}" >>"${PATH_TEASER_RUNTIME_DATAFILE}"
	done
	printf '\nAverage' >>"${PATH_TEASER_RUNTIME_DATAFILE}"
	RUNTIME_ARGUMENTS=()
	[[ ! -z ${VERBOSE+x} ]] && echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Final relative values"
	AHEAD_sub_begin
	for idx in "${TEASER_INDICES[@]}"; do
		overallAverage=$(awk '{printf "%f", $1 / $2}' <<<"${totalRelativeRuntimes[$idx]} ${numRuntimes}")
		printf '\t%s' "${overallAverage}" >>"${PATH_TEASER_RUNTIME_DATAFILE}"
		RUNTIME_ARGUMENTS+=($(awk '{printf "%.2f %.2f",$1,($1+0.25)}' <<<"${overallAverage}"))
		[[ ! -z ${VERBOSE+x} ]] && echo "${AHEAD_SCRIPT_COMMAND_PREFIX}${VARIANT_NAMES[$idx]}: ${overallAverage}"
	done
	AHEAD_sub_end
	AHEAD_sub_end
	echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Generating plot scripts"
	AHEAD_sub_begin
	[[ ! -z ${VERBOSE+x} ]] && echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Runtime"
	gnuplot_teaser_runtime "${PATH_TEASER_RUNTIME_GNUPLOTFILE}" "${PATH_TEASER_RUNTIME_PLOTFILE}" "${PATH_TEASER_RUNTIME_DATAFILE}" "${RUNTIME_ARGUMENTS[@]}"
	[[ ! -z ${VERBOSE+x} ]] && echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Memory Consumption"
	gnuplot_teaser_consumption "${PATH_TEASER_CONSUMPTION_GNUPLOTFILE}" "${PATH_TEASER_CONSUMPTION_PLOTFILE}" "${PATH_TEASER_CONSUMPTION_DATAFILE}" "${CONSUMPTION_ARGUMENTS[@]}"
	[[ ! -z ${VERBOSE+x} ]] && echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Key"
	gnuplot_teaser_legend "${PATH_TEASER_LEGEND_GNUPLOTFILE}" "${PATH_TEASER_LEGEND_PLOTFILE}" "${PATH_TEASER_CONSUMPTION_DATAFILE}"
	AHEAD_sub_end

	AHEAD_sync
	AHEAD_sub_end

	echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Collecting data for Scalar vs. Vectorized"
	AHEAD_sub_begin
	if printf '%s\n' "${ARCHITECTURE[@]}" | grep -qE '^_SSE$'; then
		[[ "${AHEAD_SCALAR_VS_VECTOR_QUERIES}" =~ \(([1-9][1-9] *)*\) ]] || AHEAD_quit 1 "${AHEAD_SCRIPT_COMMAND_PREFIX}Variable 'AHEAD_SCALAR_VS_VECTOR_QUERIES' must match pattern '\(([1-9][1-9] *)*\)' !!!"
		[[ "${AHEAD_SCALAR_VS_VECTOR_VARIANT_INDICES}" =~ \(([0-9]+ *)*\) ]] || AHEAD_quit 1 "${AHEAD_SCRIPT_COMMAND_PREFIX}Variable 'AHEAD_SCALAR_VS_VECTOR_VARIANT_INDICES' must match pattern '\(([0-9]+ *)*\)' !!!"
		eval "AHEAD_SCALAR_VS_VECTOR_QUERIES=${AHEAD_SCALAR_VS_VECTOR_QUERIES}" # use this to emulate exporting arrays.
		eval "AHEAD_SCALAR_VS_VECTOR_VARIANT_INDICES=${AHEAD_SCALAR_VS_VECTOR_VARIANT_INDICES}" # use this to emulate exporting arrays.
		numQueries="${#AHEAD_SCALAR_VS_VECTOR_QUERIES[@]}"
		ARCH="_SSE"
		AVERAGE_RUNTIMES_SCALAR=()
		AVERAGE_RUNTIMES_VECTOR=()
		AVERAGE_RUNTIMES_SCALAR_TO_VECTOR=()
		for idx in "${!VARIANTS[@]}"; do
			AVERAGE_RUNTIMES_SCALAR+=(0)
			AVERAGE_RUNTIMES_VECTOR+=(0)
			AVERAGE_RUNTIMES_SCALAR_TO_VECTOR+=(0)
		done
		for idx in "${!AHEAD_SCALAR_VS_VECTOR_QUERIES[@]}"; do
			BASE2="${BASE}${AHEAD_SCALAR_VS_VECTOR_QUERIES[$idx]}"
			REPORT_NORMALIZED_SCALAR_DATA_PATH="${PATH_EVALREPORT}/${BASE2}_scalar.data"
			REPORT_NORMALIZED_VECTOR_DATA_PATH="${PATH_EVALREPORT}/${BASE2}${ARCH}.data"
			if [[ ! -z ${VERBOSE+x} ]]; then
				echo "${AHEAD_SCRIPT_COMMAND_PREFIX}${AHEAD_SCALAR_VS_VECTOR_QUERIES[$idx]}"
				AHEAD_sub_begin
				echo "${AHEAD_SCRIPT_COMMAND_PREFIX}${REPORT_NORMALIZED_SCALAR_DATA_PATH}"
				echo "${AHEAD_SCRIPT_COMMAND_PREFIX}${REPORT_NORMALIZED_VECTOR_DATA_PATH}"
				AHEAD_sub_end
			fi
			# The following awk script computes all relative runtimes with respect to Unprotected Vector (SSE) baseline
			# The results are the sums for all considered queries (11, 12, and 13 for the paper)
			# In the awk script afterwards, we compute the average by dividing by the number of queries (3 for the paper)
			numVariants=${#VARIANTS[@]}
			ARRAY_RUNTIMES=($(awk -v query=${AHEAD_SCALAR_VS_VECTOR_QUERIES[$idx]} -v numvar=${numVariants} -v numsf=$((${AHEAD_SCALEFACTOR_MAX} - (${AHEAD_SCALEFACTOR_MIN} - 1))) \
				'BEGIN{
					maxNF=0
					numLines=0
					#printf "   * Starting awk for query %.1f. numvar=%s. numsf=%s.\n",(query/10),numvar,numsf
					for (var=1; var<=numvar; ++var) {
						totalRelativesScalar[var]=0
						totalRelativesSSE[var]=0
						for (sf=1; sf<=numsf; ++sf) {
							runtimesSSE[var,sf]=0
							relativesScalar[var,sf]=0
							relativesSSE[var,sf]=0
						}
					}
				}
				FNR==NR{
					if (FNR>1 && NF>0) {
						if (NF>maxNF) maxNF=NF
						++numLines
						for (var=1; var<=numvar; ++var) {
							runtimesSSE[var,numLines]=$(var+1)
							# we compute the SSE relative times already in the first run
							relativesSSE[var,numLines]=$(var+1)/runtimesSSE[1,numLines]
						}
					}
					next
				}
				{
					if (FNR>1 && NF>0) {
						for (var=1; var<=numvar; ++var) {
							# we compute the Scalar relative times in the second run
							relativesScalar[var,(FNR-1)]=$(var+1)/runtimesSSE[1,(FNR-1)]
						}
					}
				}
				END{
					if (numLines != numsf) {printf "ERROR: numLines(%s) != numsf(%s)\n", numLines, numsf; exit 1}
					if (maxNF != (numvar+1)) {printf "ERROR: maxNF(%s) != numvar+1(%s)\n", maxNF, (numvar+1); exit 1}
					for (var=1; var<=numvar; ++var) {
						for (sf=1; sf<=numsf; ++sf) {
							totalRelativesScalar[var] += relativesScalar[var,sf]
							totalRelativesSSE[var] += relativesSSE[var,sf]
						}
						totalRelativesScalar[var] /= numsf;
						totalRelativesSSE[var] /= numsf;
					}
					#printf "     * Runtimes relative to Unprotected SSE: Scalar ["
					for (var=1; var<=numvar; ++var) {printf "%.5f ", (totalRelativesScalar[var])}
					#printf "] SSE ["
					for (var=1; var<=numvar; ++var) {printf "%.5f ", (totalRelativesSSE[var])}
					#printf "]\n"
				}' "${REPORT_NORMALIZED_VECTOR_DATA_PATH}" "${REPORT_NORMALIZED_SCALAR_DATA_PATH}"))
			[[ "${ARRAY_RUNTIMES[@]}" == ERROR:* ]] && AHEAD_quit 1 "${ARRAY_RUNTIMES[@]}"
			RUNTIMES_SCALAR=(${ARRAY_RUNTIMES[@]:0:${numVariants}})
			RUNTIMES_VECTOR=(${ARRAY_RUNTIMES[@]:${numVariants}:${numVariants}})
			ARRAY_RUNTIMES=($(awk \
				'FNR==1{for (i=1; i<=NF; ++i) scalar[i]=$i}
				FNR==2{for (i=1; i<=NF; ++i) {vector[i]=$i; printf "%.5f ",(scalar[i] / vector[i])}}
				FNR==3{for (i=1; i<=NF; ++i) printf "%.5f ",($i+scalar[i])}
				FNR==4{for (i=1; i<=NF; ++i) printf "%.5f ",($i+vector[i])}
				FNR==5{}' << EOM
${RUNTIMES_SCALAR[@]}
${RUNTIMES_VECTOR[@]}
${AVERAGE_RUNTIMES_SCALAR[@]}
${AVERAGE_RUNTIMES_VECTOR[@]}
EOM
))
			RUNTIMES_SCALAR_TO_VECTOR=("${ARRAY_RUNTIMES[@]:0:$numVariants}")
			AVERAGE_RUNTIMES_SCALAR=("${ARRAY_RUNTIMES[@]:$numVariants:$numVariants}")
			AVERAGE_RUNTIMES_VECTOR=("${ARRAY_RUNTIMES[@]:$((numVariants*2)):$numVariants}")
			if [[ ! -z ${VERBOSE+x} ]]; then
				AHEAD_sub_begin
				echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Scalar: ${RUNTIMES_SCALAR[@]}"
				echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Vector: ${RUNTIMES_VECTOR[@]}"
				echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Scalar->Vector: ${RUNTIMES_SCALAR_TO_VECTOR[@]}"
				echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Average Scalar: ${AVERAGE_RUNTIMES_SCALAR[@]}"
				echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Average Vector: ${AVERAGE_RUNTIMES_VECTOR[@]}"
				AHEAD_sub_end
			fi
		done
		AVERAGES=($(awk -v numQueries=${numQueries} \
		'FNR==1{for (i=1; i<=NF; ++i) {scalar[i]=$i/numQueries; printf " %.5f",scalar[i]}}
		FNR==2{for (i=1; i<=NF; ++i) {vector[i]=$i/numQueries; printf " %.5f",vector[i]} for (i=1; i<=NF; ++i) {printf " %.1f",(scalar[i]/vector[i])}}' << EOM
${AVERAGE_RUNTIMES_SCALAR[@]}
${AVERAGE_RUNTIMES_VECTOR[@]}
EOM
		))
		AVERAGE_RUNTIMES_SCALAR=("${AVERAGES[@]:0:$numVariants}")
		AVERAGE_RUNTIMES_VECTOR=("${AVERAGES[@]:$numVariants:$numVariants}")
		AVERAGE_RUNTIMES_SCALAR_TO_VECTOR=("${AVERAGES[@]:$((numVariants*2)):$numVariants}")
		if [[ ! -z ${VERBOSE+x} ]]; then
			echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Total"
			AHEAD_sub_begin
			echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Scalar: ${AVERAGE_RUNTIMES_SCALAR[@]}"
			echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Vector: ${AVERAGE_RUNTIMES_VECTOR[@]}"
			echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Scalar->Vector: ${AVERAGE_RUNTIMES_SCALAR_TO_VECTOR[@]}"
			AHEAD_sub_end
		fi
		echo -e "Variant\tScalar\tVectorized" >"${PATH_SCALAR_VS_VECTOR_DATAFILE}"
		SCALE="10000"
		THRESHOLD=$(bc <<< "scale=0;${AHEAD_SCALAR_VS_VECTOR_YMAX}*${SCALE}/1")
		VALUES_TOO_LARGE=()
		POSITIONS_TOO_LARGE=()
		for idx in "${!AVERAGE_RUNTIMES_SCALAR[@]}"; do
			echo -e "${VARIANT_NAMES[$idx]}\t${AVERAGE_RUNTIMES_SCALAR[$idx]}\t${AVERAGE_RUNTIMES_VECTOR[$idx]}" >>"${PATH_SCALAR_VS_VECTOR_DATAFILE}"
			SCALAR=$(bc <<< "scale=0;${AVERAGE_RUNTIMES_SCALAR[$idx]}*${SCALE}/1")
			VECTOR=$(bc <<< "scale=0;${AVERAGE_RUNTIMES_VECTOR[$idx]}*${SCALE}/1")
			((SCALAR > THRESHOLD)) && { VALUES_TOO_LARGE+=("${AVERAGE_RUNTIMES_SCALAR[$idx]}"); POSITIONS_TOO_LARGE+=("$(bc <<< "$idx-0.4")"); }
		done
		[[ ! -z ${VERBOSE+x} ]] && echo "     * Too large: values=[${VALUES_TOO_LARGE[@]}] positions=[${POSITIONS_TOO_LARGE[@]}]"
		ARGUMENTS=("set yrange [${AHEAD_SCALAR_VS_VECTOR_YMIN}:${AHEAD_SCALAR_VS_VECTOR_YMAX}]")
		for idx in "${!VALUES_TOO_LARGE[@]}"; do
			ARGUMENTS+=("set label \"$(awk '{printf "%.2f",$1}' <<< "${VALUES_TOO_LARGE[$idx]}")\" at first ${POSITIONS_TOO_LARGE[$idx]}, screen 0.97")
		done
		for idx in "${!AVERAGE_RUNTIMES_SCALAR[@]}"; do
			SCALAR="${AVERAGE_RUNTIMES_SCALAR[$idx]}"
			VECTOR="${AVERAGE_RUNTIMES_VECTOR[$idx]}"
			Xa="$(bc <<< "$idx-0.15")"
			(( $(bc <<< "$SCALAR <= ${AHEAD_SCALAR_VS_VECTOR_YMAX}") )) && Ya="$SCALAR" || Ya="$(bc <<< "scale=2;${AHEAD_SCALAR_VS_VECTOR_YMAX}/1")"
			Xb="$(bc <<< "$idx+0.15")"
			Yb="${VECTOR}"
			ARGUMENTS+=("set arrow from first $Xa,$Ya to first $Xb,$Yb head filled front")
			X="$(bc <<< "$idx+0.2")"
			Y="$(bc <<< "scale=2; $Yb + ($Ya - $Yb) / 2.00")"
			ARGUMENTS+=("set label \"${AVERAGE_RUNTIMES_SCALAR_TO_VECTOR[$idx]}\" at first $X,$Y rotate by -70")
		done

		echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Generating plot scripts"
		if [[ ! -z ${VERBOSE+x} ]]; then
			AHEAD_sub_begin
			echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Arguments to gnuplot_scalarVSvector:";
			AHEAD_sub_begin
			for V in "${ARGUMENTS[@]}"; do
				echo "${AHEAD_SCRIPT_COMMAND_PREFIX}$V"
			done
			AHEAD_sub_end
			AHEAD_sub_end
		fi
		gnuplot_scalarVSvector "${PATH_SCALAR_VS_VECTOR_GNUPLOTFILE}" "${PATH_SCALAR_VS_VECTOR_PLOTFILE}" "${PATH_SCALAR_VS_VECTOR_DATAFILE}" "${ARGUMENTS[@]}"
	else
		echo "${AHEAD_SCRIPT_COMMAND_PREFIX}SSE architecture not found, not specified, or disabled (or the like)! Skipping."
	fi

	echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Plotting"
	AHEAD_sub_begin
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
			echo "${AHEAD_SCRIPT_COMMAND_PREFIX}${BASE3}"
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

		echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Creating tex/PDF files for normalized averages (${EVAL_NORMALIZEDALLTEXFILE_PATH})"
		gnuplotcodetex  ${EVAL_NORMALIZEDALLPLOTFILE_PATH} ${EVAL_NORMALIZEDALLTEXFILE_PATH} ${EVAL_NORMALIZEDALLDATAFILE_PATH} \
			"set yrange [0:]" "set ytics out" "set xtics out" "set grid noxtics ytics" "unset xlabel" "set ylabel \"Relative Runtime\""
		gnuplot ${EVAL_NORMALIZEDALLPLOTFILE_PATH}
	done
	AHEAD_sub_end

	echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Teaser graphs"
	gnuplot "${PATH_TEASER_RUNTIME_GNUPLOTFILE}"
	gnuplot "${PATH_TEASER_CONSUMPTION_GNUPLOTFILE}"
	gnuplot "${PATH_TEASER_LEGEND_GNUPLOTFILE}"

	echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Scalar vs. Vector graph"
	gnuplot "${PATH_SCALAR_VS_VECTOR_GNUPLOTFILE}"

	AHEAD_sync
	AHEAD_sub_end
else
	echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Skipping evaluation."
fi

# Verification
if ((DO_VERIFY != 0)); then
	echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Verifying."
	AHEAD_sub_begin
	date
	NUMARCHS="${#ARCHITECTURE[@]}"
	isAnyBad=0
	for idxArch in $(seq 0 $(echo "${NUMARCHS}-1" | bc)); do
		ARCH=${ARCHITECTURE[$idxArch]}
		ARCHNAME=${ARCHITECTURE_NAMES[$idxArch]}
		echo "${AHEAD_SCRIPT_COMMAND_PREFIX}${ARCHNAME}"
		AHEAD_sub_begin
		for NUM in "${IMPLEMENTED[@]}"; do
			echo -n "${AHEAD_SCRIPT_COMMAND_PREFIX}Q${NUM}:"
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
		AHEAD_sub_end
	done

	if ((isAnyBad == 1)); then
		echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Generating diffs:"
		for s in "${ARCHITECTURE[@]}"; do
			for q in "${IMPLEMENTED[@]}"; do
				for v in "${VARIANTS[@]}"; do
					for t in out err; do
						grepfile="./grep.${t}"
						diff "${PATH_EVALDATA}/${BASE}${q}_normal_scalar.${t}" "${PATH_EVALDATA}/${BASE}${q}${v}${s}.${t}" | grep result >${grepfile}
						if [[ -s "${grepfile}" ]]; then
							echo "${AHEAD_SCRIPT_COMMAND_PREFIX}-------------------------------"
							echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Q${q}${v}${s} (${t}):"
							cat "${grepfile}"
							echo "${AHEAD_SCRIPT_COMMAND_PREFIX}==============================="
						fi
						rm -f ${grepfile}
					done
				done
			done
		done
	fi
	AHEAD_sub_end
else
	echo "${AHEAD_SCRIPT_COMMAND_PREFIX}Skipping verification."
fi
