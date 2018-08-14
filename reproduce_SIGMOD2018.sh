#!/bin/bash

# This script is an all-in-one solution to reproduce the results from our SIGMOD 2018 paper titled "AHEAD: Adaptable Data Hardening for On-the-Fly Hardware Error Detection during Database Query Processing"

./generate_ssbdata.sh || exit 1

pushd eval
# source run.conf
./run.sh || (popd && exit 1)
mv "${PATH_EVAL_CURRENT}/data" "${PATH_EVAL_CURRENT}/report" "${AHEAD_PAPER_RESULTS_SSB}"
popd

pushd paper
pdflatex sigmod2018.tex || (popd && exit 1)
popd

