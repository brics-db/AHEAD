#!/bin/bash

# This script is an all-in-one solution to reproduce the results from our SIGMOD 2018 paper titled "AHEAD: Adaptable Data Hardening for On-the-Fly Hardware Error Detection during Database Query Processing"

if [[ $(id -u) -eq 0 ]]; then
	echo "[ERROR] You must not run this script as super user!"
	exit 1
fi

# Allow to run individual steps
ARGS=("$@")
if [[ $# > 0 ]] ; then
	case "${ARGS[0]}" in
		GENERATE)
			PHASE="GENERATE"
			DO_GENERATE=1
			DO_SSB=0
			DO_CODINGBENCHMARK=0
			DO_MODULARINVERSE=0
			;;
		SSB)
			DO_GENERATE=0
			DO_SSB=1
			DO_CODINGBENCHMARK=0
			DO_MODULARINVERSE=0
			;;
		CB)
			DO_GENERATE=0
			DO_SSB=0
			DO_CODINGBENCHMARK=1
			DO_MODULARINVERSE=0
			;;
		INV)
			DO_GENERATE=0
			DO_SSB=0
			DO_CODINGBENCHMARK=0
			DO_MODULARINVERSE=1
			;;
		ALL) ;&
		*)
			PHASE="ALL"
			;;
	esac
fi

# set the step switches if not done yet
[ -z ${DO_GENERATE+x} ] && DO_GENERATE=1
[ -z ${DO_SSB+x} ] && DO_SSB=1
[ -z ${DO_CODINGBENCHMARK+x} ] && DO_CODINGBENCHMARK=1
[ -z ${DO_MODULARINVERSE+x} ] && DO_MODULARINVERSE=1

echo "######################################################"
echo "# Welcome to the SIGMOD 2018 reproducibility script. #"
echo "######################################################"
echo

echo "######################################################"
echo "# Initializing, syncing and updating git submodules. #"
echo "######################################################"
git submodule update --init --recursive
git submodule sync --recursive
git submodule update --recursive

if [[ ${DO_GENERATE} == 1 ]]; then
	echo "###########################################################"
	echo "# Running Star Schema Benchmark Data Generation           #"
	echo "###########################################################"
	./generate_ssbdata.sh || exit 1
	echo
fi

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
	modes=($(sudo ./scalinggovernor.sh avail 0))
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

export reproscript=1

if [[ ${DO_SSB} == 1 ]]; then
	echo "###########################################################"
	echo "# Running Star Schema Benchmark                           #"
	echo "###########################################################"
	pushd eval &>/dev/null
	#export DATE="2018-08-15_17-04" # for testing
	#source ./run.conf
	./run.sh || exit 1
	rm -Rf "${AHEAD_PAPER_RESULTS_SSB}/data" "${AHEAD_PAPER_RESULTS_SSB}/report"
	ln -s "${PATH_EVAL_CURRENT}/data" "../${AHEAD_PAPER_RESULTS_SSB}/data"
	ln -s "${PATH_EVAL_CURRENT}/report" "../${AHEAD_PAPER_RESULTS_SSB}/report"
	popd &>/dev/null
	echo
fi

if [[ ${DO_CODINGBENCHMARK} == 1 ]]; then
	echo "###########################################################"
	echo "# Running Coding Benchmark                                #"
	echo "###########################################################"
	./coding_benchmark.sh
fi

if [[ ${DO_MODULARINVERSE} == 1 ]]; then
	echo "###########################################################"
	echo "# Running Modular Inverse Benchmark                       #"
	echo "###########################################################"
	./modular_inverse.sh
fi

pushd paper &>/dev/null
(pdflatex sigmod2018.tex && pdflatex sigmod2018.tex) || exit 1
popd &>/dev/null

