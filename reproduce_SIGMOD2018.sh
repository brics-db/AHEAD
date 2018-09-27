#!/bin/bash

# This script is an all-in-one solution to reproduce the results from our SIGMOD 2018 paper titled "AHEAD: Adaptable Data Hardening for On-the-Fly Hardware Error Detection during Database Query Processing"

if [[ $(id -u) -eq 0 ]]; then
	echo "[ERROR] You must not run this script as super user!"
	exit 1
fi

# Allow to run individual steps
#
# Set special variable 'NO_BENCH=1' to not do any actual benchmarking
#
ARGS=("$@")
if [[ $# > 0 ]] ; then
	PHASE="${ARGS[0]}"
	case "${ARGS[0]}" in
		GENERATE)
			DO_SUBMODULE=1
			DO_GENERATE=1
			DO_SSB=0
			DO_CODINGBENCHMARK=0
			DO_MODULARINVERSE=0
			DO_PLOT=1
			;;
		SSB)
			DO_SUBMODULE=0
			DO_GENERATE=0
			DO_SSB=1
			DO_CODINGBENCHMARK=0
			DO_MODULARINVERSE=0
			DO_PLOT=0
			;;
		CB) ;&
		MB)
			DO_SUBMODULE=0
			DO_GENERATE=0
			DO_SSB=0
			DO_CODINGBENCHMARK=1
			DO_MODULARINVERSE=0
			DO_PLOT=0
			;;
		INV) ;&
		MODINV)
			DO_SUBMODULE=0
			DO_GENERATE=0
			DO_SSB=0
			DO_CODINGBENCHMARK=0
			DO_MODULARINVERSE=1
			DO_PLOT=0
			;;
		PLOT)
			DO_SUBMODULE=0
			DO_GENERATE=0
			DO_SSB=0
			DO_CODINGBENCHMARK=0
			DO_MODULARINVERSE=0
			DO_PLOT=1
			;;
		ALL) ;&
		DEFAULT)
			PHASE="ALL"
			;;
		*)
			PHASE="'${ARGS[0]}' Unknown -- Reverting to ALL"
			;;
	esac
fi

[[ -z "${PHASE+x}" ]] && PHASE=ALL

# set the step switches if not done yet
[ -z ${DO_SUBMODULE+x} ] && DO_SUBMODULE=1
[ -z ${DO_GENERATE+x} ] && DO_GENERATE=1
[ -z ${DO_SSB+x} ] && DO_SSB=1
[ -z ${DO_CODINGBENCHMARK+x} ] && DO_CODINGBENCHMARK=1
[ -z ${DO_MODULARINVERSE+x} ] && DO_MODULARINVERSE=1
[ -z ${DO_PLOT+x} ] && DO_PLOT=1
[ -z ${NO_BENCH+x} ] && NO_BENCH=0

echo "######################################################"
echo "# Welcome to the SIGMOD 2018 reproducibility script. #"
echo "######################################################"
echo
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)/common.conf"
echo "${AHEAD_SCRIPT_ECHO_INDENT}Running Phase \"${PHASE}\""
echo

if ((DO_SUBMODULE != 0)); then
	echo "######################################################"
	echo "# Initializing, syncing and updating git submodules. #"
	echo "######################################################"
	echo
	echo -n "${AHEAD_SCRIPT_ECHO_INDENT}git submodule update --init --recursive..."
	AHEAD_run_hidden_output git submodule update --init --recursive || exit 1
	echo -n "${AHEAD_SCRIPT_ECHO_INDENT}git submodule sync --recursive..."
	AHEAD_run_hidden_output git submodule sync --recursive || exit 1
	echo -n "${AHEAD_SCRIPT_ECHO_INDENT}git submodule update --recursive..."
	AHEAD_run_hidden_output git submodule update --recursive || exit 1
	AHEAD_sync
fi

if ((DO_GENERATE != 0)) && ((NO_BENCH == 0)); then
	echo "###########################################################"
	echo "# Running Star Schema Benchmark Data Generation           #"
	echo "###########################################################"
	./generate_ssbdata.sh || exit 1
	AHEAD_sync
	echo
	AHEAD_sub_reset
fi

AHEAD_prepare_scalinggovernor_and_turboboost

export reproscript=1

if ((DO_SSB != 0)); then
	echo "###########################################################"
	echo "# Running Star Schema Benchmark                           #"
	echo "###########################################################"
	AHEAD_pushd "eval"

	if [[ -z ${NO_BENCH+x} ]] || ((NO_BENCH == 0)); then
		./run.sh || exit 1
		echo -n "${AHEAD_DATE}" >"${AHEAD_PREVIOUS_DATE_FILE}"

		for minbfw in $(seq ${AHEAD_MINBFW_MIN} ${AHEAD_MINBFW_MAX}); do
			DIRSUFFIX="${AHEAD_MINBFW_SUFFIX}$minbfw"
			if ((minbfw < 4)); then
				# We don't need the plotting feature for the minbfw runs
				# AHEAD_DATE="2018-09-19_13-10" 
				AHEAD_VARIANTS="('_normal' '_continuous')" AHEAD_VARIANT_NAMES="('Unprotected' 'Continuous')" AHEAD_IMPLEMENTED="(11)" AHEAD_SCALEFACTOR_MIN=1 AHEAD_SCALEFACTOR_MAX=1 DO_EVAL_TEASER=0 DO_EVAL_SCALARVSVECTOR=0 DO_EVAL_PLOT=0 AHEAD_BENCHMARK_DBDIR_SUFFIX="${DIRSUFFIX}" AHEAD_BENCHMARK_EVALDIR_SUFFIX="${DIRSUFFIX}" AHEAD_BENCHMARK_MINBFW=$minbfw ./run.sh || exit 1
			elif ((minbfw == 4)); then
				AHEAD_VARIANTS="('_normal' '_continuous')" AHEAD_VARIANT_NAMES="('Unprotected' 'Continuous')" AHEAD_IMPLEMENTED="(11)" AHEAD_SCALEFACTOR_MIN=1 AHEAD_SCALEFACTOR_MAX=1 DO_EVAL_TEASER=0 DO_EVAL_SCALARVSVECTOR=0 DO_EVAL_PLOT=0 AHEAD_BENCHMARK_DBDIR_SUFFIX="${DIRSUFFIX}" AHEAD_BENCHMARK_EVALDIR_SUFFIX="${DIRSUFFIX}" AHEAD_BENCHMARK_MINBFW=$minbfw AHEAD_BENCHMARK_MINBFW_EXECUTABLE_SUFFIX="${AHEAD_RESTINY32_SUFFIX}" ./run.sh || exit 1
			else
				echo"${AHEAD_MINBFW_SUFFIX}[ERROR] The current implementation only supports minbfw between 1 and 4."
			fi
		done
		AHEAD_sync
	else
		### No benchmarking, but re-create evaluation data
		if [[ ! -f "${AHEAD_PREVIOUS_DATE_FILE}" ]]; then
			echo "${AHEAD_SCRIPT_ECHO_INDENT}WARNING: It seems that there is no previous run of AHEAD, because \"${AHEAD_PREVIOUS_DATE_FILE}\" does not exist.\n   Please, either first run this reproducibility script at least once, or create the file with appropriate contents in the format yyyy-MM-dd_HH-mm, e.g. 2018-09-13_17-50 !" >&2
			exit 1
		fi
		export AHEAD_DATE=$(cat "${AHEAD_PREVIOUS_DATE_FILE}")
		[[ ! -z ${VERBOSE+x} ]] && echo "${AHEAD_SCRIPT_ECHO_INDENT}AHEAD_DATE=${AHEAD_DATE}"
		./run.sh EVAL || exit 1
        
		for minbfw in $(seq ${AHEAD_MINBFW_MIN} ${AHEAD_MINBFW_MAX}); do
			DIRSUFFIX="${AHEAD_MINBFW_SUFFIX}$minbfw"
			if ((minbfw < 4)); then
				AHEAD_VARIANTS="('_normal' '_continuous')" AHEAD_VARIANT_NAMES="('Unprotected' 'Continuous')" AHEAD_IMPLEMENTED="(11)" AHEAD_SCALEFACTOR_MIN=1 AHEAD_SCALEFACTOR_MAX=1 DO_BENCHMARK=0 DO_EVAL_TEASER=0 DO_EVAL_SCALARVSVECTOR=0 DO_EVAL_PLOT=0 AHEAD_BENCHMARK_DBDIR_SUFFIX="${DIRSUFFIX}" AHEAD_BENCHMARK_EVALDIR_SUFFIX="${DIRSUFFIX}" AHEAD_BENCHMARK_MINBFW=$minbfw ./run.sh || exit 1
			elif ((minbfw == 4)); then
				AHEAD_VARIANTS="('_normal' '_continuous')" AHEAD_VARIANT_NAMES="('Unprotected' 'Continuous')" AHEAD_IMPLEMENTED="(11)" AHEAD_SCALEFACTOR_MIN=1 AHEAD_SCALEFACTOR_MAX=1 DO_BENCHMARK=0 DO_EVAL_TEASER=0 DO_EVAL_SCALARVSVECTOR=0 DO_EVAL_PLOT=0 AHEAD_BENCHMARK_DBDIR_SUFFIX="${DIRSUFFIX}" AHEAD_BENCHMARK_EVALDIR_SUFFIX="${DIRSUFFIX}" AHEAD_BENCHMARK_MINBFW=$minbfw AHEAD_BENCHMARK_MINBFW_EXECUTABLE_SUFFIX="${AHEAD_RESTINY32_SUFFIX}" ./run.sh || exit 1
			else
				echo"${AHEAD_MINBFW_SUFFIX}[ERROR] The current implementation only supports minbfw between 1 and 4."
			fi
		done
		AHEAD_sync
	fi

	# Link the report files generated by eval/run.sh script to the paper result SSB folder
	echo "${AHEAD_SCRIPT_ECHO_INDENT}Creating symlinks"
	AHEAD_sub_begin
	(
		source run.conf
		[[ ! -z ${VERBOSE+x} ]] && echo "${AHEAD_SCRIPT_ECHO_INDENT}PWD: '$(pwd)'"
		echo "${AHEAD_SCRIPT_ECHO_INDENT}linking '${PATH_EVALDATA}' <- '${AHEAD_PAPER_RESULTS_SSB}/data'" && ln -fs "${PATH_EVALDATA}" "${AHEAD_PAPER_RESULTS_SSB}/data"
		echo "${AHEAD_SCRIPT_ECHO_INDENT}linking '${PATH_EVALREPORT}' <- '${AHEAD_PAPER_RESULTS_SSB}/report'" && ln -fs "${PATH_EVALREPORT}" "${AHEAD_PAPER_RESULTS_SSB}/report"
		echo "${AHEAD_SCRIPT_ECHO_INDENT}linking '${PATH_EVALINTER}' <- '${AHEAD_PAPER_RESULTS_SSB}/intermediate'" && ln -fs "${PATH_EVALINTER}" "${AHEAD_PAPER_RESULTS_SSB}/intermediate"
		if [[ ! -z ${PATH_TEASER_RUNTIME_BASENAME+x} ]]; then
			find "$(pwd)" -iname "${PATH_TEASER_RUNTIME_BASENAME}"'*' -print0 | \
			xargs -0 -I file bash -c "link=\"${AHEAD_PAPER_RESULTS_SSB}/\$(basename 'file')\"; echo \"${AHEAD_SCRIPT_ECHO_INDENT}linking 'file' <- '\${link}'\"; rm -f \"\${link}\"; ln -s \"file\" \"\${link}\""
		else
			echo "${AHEAD_SCRIPT_ECHO_INDENT}[ERROR] PATH_TEASER_RUNTIME_BASENAME not set!"
		fi
		if [[ ! -z ${PATH_TEASER_CONSUMPTION_BASENAME+x} ]]; then
			find "$(pwd)" -iname "${PATH_TEASER_CONSUMPTION_BASENAME}"'*' -print0 | \
			xargs -0 -I file bash -c "link=\"${AHEAD_PAPER_RESULTS_SSB}/\$(basename 'file')\"; echo \"${AHEAD_SCRIPT_ECHO_INDENT}linking 'file' <- '\${link}'\"; rm -f \"\${link}\"; ln -s \"file\" \"\${link}\""
		else
			echo "${AHEAD_SCRIPT_ECHO_INDENT}[ERROR] PATH_TEASER_CONSUMPTION_BASENAME not set!"
		fi
		if [[ ! -z ${PATH_TEASER_LEGEND_BASENAME+x} ]]; then
			find "$(pwd)" -iname "${PATH_TEASER_LEGEND_BASENAME}"'*' -print0 | \
			xargs -0 -I file bash -c "link=\"${AHEAD_PAPER_RESULTS_SSB}/\$(basename 'file')\"; echo \"${AHEAD_SCRIPT_ECHO_INDENT}linking 'file' <- '\${link}'\"; rm -f \"\${link}\"; ln -s \"file\" \"\${link}\""
		else
			echo "${AHEAD_SCRIPT_ECHO_INDENT}[ERROR] PATH_TEASER_LEGEND_BASENAME not set!"
		fi
		if [[ ! -z ${PATH_SCALAR_VS_VECTOR_BASENAME+x} ]]; then
			find "$(pwd)" -iname "${PATH_SCALAR_VS_VECTOR_BASENAME}"'*' -print0 | \
			xargs -0 -I file bash -c "link=\"${AHEAD_PAPER_RESULTS_SSB}/\$(basename 'file')\"; echo \"${AHEAD_SCRIPT_ECHO_INDENT}linking 'file' <- '\${link}'\"; rm -f \"\${link}\"; ln -s \"file\" \"\${link}\""
		else
			echo "${AHEAD_SCRIPT_ECHO_INDENT}[ERROR] PATH_SCALAR_VS_VECTOR_BASENAME not set!"
		fi
		#find "$(pwd)" \( -iname "${PATH_TEASER_RUNTIME_BASENAME}"'*' -o -iname "${PATH_TEASER_CONSUMPTION_BASENAME}"'*' -o -iname "${PATH_TEASER_LEGEND_BASENAME}"'*' -o -iname "${PATH_SCALAR_VS_VECTOR_BASENAME}"'*' \) -print0 | \
		#	xargs -0 -I file bash -c "link=\"${AHEAD_PAPER_RESULTS_SSB}/\$(basename 'file')\"; echo \" * linking 'file' <- '\${link}'\"; rm -f \"\${link}\"; ln -s \"file\" \"\${link}\""
	)
	AHEAD_sub_end

	# norm-all_scalar.data
	echo "${AHEAD_SCRIPT_ECHO_INDENT}Processing minBFW data"
	AHEAD_sub_begin
	echo "${AHEAD_SCRIPT_ECHO_INDENT}${PATH_MINBFW_RUNTIME_DATAFILE}"
	MINBFW_RUNTIME_FILENAME_SCALAR="norm-all_scalar.data"
	MINBFW_RUNTIME_FILENAME_SSE="norm-all_SSE.data"
	echo -e "MinBFW\tScalar\tSSE4.2" >"${PATH_MINBFW_RUNTIME_DATAFILE}"
	rm -f "${PATH_MINBFW_RUNTIME_DATAFILE} ${PATH_MINBFW_CONSUMPTION_DATAFILE}"
	for minbfw in $(seq ${AHEAD_MINBFW_MIN} ${AHEAD_MINBFW_MAX}); do
		echo -n "$minbfw" >>"${PATH_MINBFW_RUNTIME_DATAFILE}"
		path="${AHEAD_DATE}${AHEAD_MINBFW_SUFFIX}$minbfw/report"
		awk 'BEGIN{total=0;numLines=0} {if (FNR>1 && NF>0) {total+=$3; numLines++}} END{printf "\t%s",($3/numLines)}' "${path}/${MINBFW_RUNTIME_FILENAME_SCALAR}" >>"${PATH_MINBFW_RUNTIME_DATAFILE}"
		awk 'BEGIN{total=0;numLines=0} {if (FNR>1 && NF>0) {total+=$3; numLines++}} END{printf "\t%s\n",($3/numLines)}' "${path}/${MINBFW_RUNTIME_FILENAME_SSE}" >>"${PATH_MINBFW_RUNTIME_DATAFILE}"
	done
	totalAbsoluteStorages=(0 0 0)
	echo -e "minBFW\tUnprotected\tContinuous\tBit-Packed" >"${PATH_MINBFW_CONSUMPTION_DATAFILE}"
	for minbfw in $(seq ${AHEAD_MINBFW_MIN} ${AHEAD_MINBFW_MAX}); do
		(
			DIRSUFFIX="${AHEAD_MINBFW_SUFFIX}$minbfw"
			AHEAD_VARIANTS="('_normal' '_continuous')"
			AHEAD_VARIANT_NAMES="('Unprotected' 'Continuous')"
			AHEAD_IMPLEMENTED="(11)"
			AHEAD_BENCHMARK_EVALDIR_SUFFIX="${DIRSUFFIX}"
			AHEAD_BENCHMARK_MINBFW=$minbfw

			source run.conf

			pattern="\(([1-9][1-9] *)*\)"
			[[ "${AHEAD_IMPLEMENTED}" =~ $pattern ]] || AHEAD_exit 1 "${AHEAD_SCRIPT_ECHO_INDENT}Variable 'AHEAD_IMPLEMENTED' must match pattern '$pattern' !!!"
			pattern="\(('_[a-z_]+' *)*\)"
			[[ "${AHEAD_VARIANTS}" =~ $pattern ]] || AHEAD_exit 1 "${AHEAD_SCRIPT_ECHO_INDENT}Variable 'AHEAD_VARIANTS' must match pattern '$pattern' !!!"
			pattern="\(('[a-zA-Z]+' *)*\)"
			[[ "${AHEAD_VARIANT_NAMES}" =~ $pattern ]] || AHEAD_exit 1 "${AHEAD_SCRIPT_ECHO_INDENT}Variable 'AHEAD_VARIANT_NAMES' must match pattern '$pattern' !!!"
			eval "AHEAD_IMPLEMENTED=${AHEAD_IMPLEMENTED}" # use this to emulate exporting arrays.
			eval "AHEAD_VARIANTS=${AHEAD_VARIANTS}" # use this to emulate exporting arrays.
			eval "AHEAD_VARIANT_NAMES=${AHEAD_VARIANT_NAMES}" # use this to emulate exporting arrays.

			ARCH="_scalar"
			NUM="11"
			echo -n "$minbfw" >>"${PATH_MINBFW_CONSUMPTION_DATAFILE}"
			BASE2="${AHEAD_EXECUTABLE_BASE}${NUM}"
			# In the following, we first sum up the storage consumption (per query ${NUM}) and then divide the totals
			# We assume that file ${EVAL_FILEOPS_OUT} contains only the filtered operator stats AND
			# that the first N entries belong to the very first query. We collect all consumptions until the operator number is smaller again, which means the next query run starts, and then stop awk.
			for idx in 0 1; do
				type="${BASE2}${AHEAD_VARIANTS[$idx]}${ARCH}"
				EVAL_FILEOPS_OUT="${PATH_EVALINTER}/${type}.ops.out"
				[[ ! -z ${VERBOSE+x} ]] && echo "${AHEAD_SCRIPT_ECHO_INDENT}${EVAL_FILEOPS_OUT}"
				# use the projected consumption for the teaser
				STORAGES=($(awk \
					'BEGIN {opNumBefore=0; consumption=0; projected=0}
					opNumBefore < $1 {
						opNumBefore=$1
						consumption+=$4
						projected+=$5
						next
					}
					{exit}
					END {printf "%s %s",consumption,projected}' \
					"${EVAL_FILEOPS_OUT}"))
				[[ ! -z ${VERBOSE+x} ]] && echo "${AHEAD_SCRIPT_ECHO_INDENT}STORAGES=${STORAGES[@]}"
				((CONSUMPTION[idx] += STORAGES[0]))
				((PROJECTED[idx] += STORAGES[1]))
			done
			UNPROTECTED=$(bc <<< "scale=2;${CONSUMPTION[0]} / ${CONSUMPTION[0]}")
			CONTINUOUS=$(bc <<< "scale=2;${CONSUMPTION[1]} / ${CONSUMPTION[0]}")
			BITPACKED=$(bc <<< "scale=2;${PROJECTED[1]} / ${CONSUMPTION[0]}")
			printf '\t%s\t%s\t%s\n' "${UNPROTECTED}" "${CONTINUOUS}" "${BITPACKED}" >>"${PATH_MINBFW_CONSUMPTION_DATAFILE}"
		)
	done
	echo "Plotting minBFW graphs"
	AHEAD_pushd "${AHEAD_PAPER_RESULTS_SSB}"
	gnuplot "${PATH_MINBFW_RUNTIME_GNUPLOTFILE}"
	gnuplot "${PATH_MINBFW_CONSUMPTION_GNUPLOTFILE}"
	AHEAD_popd
	AHEAD_sub_end

	AHEAD_popd
	AHEAD_sub_reset
fi

if ((DO_CODINGBENCHMARK != 0)); then
	echo "###########################################################"
	echo "# Running Coding Benchmark                                #"
	echo "###########################################################"
	echo

	if [[ -z ${NO_BENCH+x} ]] || ((NO_BENCH == 0)); then
		bash ${AHEAD_SCRIPT_CODBEN}
	fi

	# gnuplot the results
	AHEAD_pushd "${AHEAD_PAPER_RESULTS_CB}" && gnuplot "${PATH_CODBENCH_ENCODE_GNUPLOTFILE}" && gnuplot "${PATH_CODBENCH_CHECK_GNUPLOTFILE}" && gnuplot "${PATH_CODBENCH_DECODE_GNUPLOTFILE}" && gnuplot "${PATH_CODBENCH_LABELS_GNUPLOTFILE}"
	AHEAD_popd
	AHEAD_sync

	AHEAD_sub_reset
fi

if ((DO_MODULARINVERSE != 0)); then
	echo "###########################################################"
	echo "# Running Modular Inverse Benchmark                       #"
	echo "###########################################################"
	echo

	if [[ -z ${NO_BENCH+x} ]] || ((NO_BENCH == 0)); then
		bash ${AHEAD_SCRIPT_MODINV}
	fi

	# gnuplot the results
	AHEAD_pushd "${AHEAD_PAPER_RESULTS_MI}" || AHEAD_exit $? "Could not enter directory '${AHEAD_PAPER_RESULTS_MI}'"
	AHEAD_run_hidden_output gnuplot "${PATH_MODINV_GNUPLOTFILE}" && AHEAD_echo "Plotted graphs" || AHEAD_exit $? "Error plotting graphs!"
	AHEAD_popd
	AHEAD_sync

	AHEAD_sub_reset
fi

if ((DO_PLOT != 0)); then
	echo "###########################################################"
	echo "# Generating simgod2018.pdf                               #"
	echo "###########################################################"
	AHEAD_pushd ${AHEAD_PAPER_PATH}
	(pdflatex sigmod2018.tex && pdflatex sigmod2018.tex && pdflatex sigmod2018.tex) || exit 1
	printf '\n\nDone. You can re-compile the paper by calling "pdflatex sigmod2018.tex" in subfolder "paper"\n'
	AHEAD_popd
	AHEAD_sync
fi

AHEAD_sub_reset

exit 0
