#!/bin/bash

# This script is an all-in-one solution to reproduce the results from our SIGMOD 2018 paper titled "AHEAD: Adaptable Data Hardening for On-the-Fly Hardware Error Detection during Database Query Processing"

echo "######################################################"
echo "# Welcome to the SIGMOD 2018 reproducibility script. #"
echo "######################################################"
echo

if [[ $(id -u) -eq 0 ]]; then
    echo "[ERROR] You must not run this script as super user!"
    exit 1
fi

./generate_ssbdata.sh || exit 1

echo "###########################################################"
echo "# For the following tests, for better reproducibilty, we: #"
echo "#   *  DISABLE turboboost                                 #"
echo "#   * set the OS scaling governor to PERFORMANCE          #"
echo "#                                                         #"
echo "# For that, you need a sudoer account!                    #"
echo "#                                                         #"

echo -n "#   * turboboost: "
haserror=0
(sudo ./turboboost.sh disable &> /dev/null || (echo "failed.                                 #"; export haserror=1))
if [[ $haserror == 0 ]]; then
	echo "succeeded.                              #"
fi
echo -n "#   * scaling governor: "
haserror=0
modes=($(sudo ./scalinggovernor.sh avail 0))
hasperformance=0
for mode in "${modes[@]}"; do
    if [[ "${mode}" == performance ]]; then
        hasperformance=1
        (sudo ./scalinggovernor.sh set performance &> /dev/null ||  (echo "[WARNING] Could not set the scalinggovernor!"; export haserror=1))
    fi
done
[[ $hasperformance = 0 ]] && (echo "failed.                           #"; echo "[WARNING] I was looking for governor \"performance\", but could not find it!"; export haserror=1)
if [[ $haserror == 0 ]]; then
	echo "succeeded.                        #"
fi

echo "###########################################################"

pushd eval
#export DATE="2018-08-15_17-04" # for testing
source ./run.conf
./run.sh || (popd && exit 1)
rm -Rf "${AHEAD_PAPER_RESULTS_SSB}/data" "${AHEAD_PAPER_RESULTS_SSB}/report"
ln -s "${PATH_EVAL_CURRENT}/data" "../${AHEAD_PAPER_RESULTS_SSB}/data"
ln -s "${PATH_EVAL_CURRENT}/report" "../${AHEAD_PAPER_RESULTS_SSB}/report"
popd

echo "###########################################################"
echo "# Running Coding Benchmarks and Modulo Inverse Benchmark  #"
echo "###########################################################"
./coding_benchmark.sh
./modular_inverse.sh

pushd paper
pdflatex sigmod2018.tex || (popd && exit 1)
popd

