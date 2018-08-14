#!/bin/bash

# This script is an all-in-one solution to reproduce the results from our SIGMOD 2018 paper titled "AHEAD: Adaptable Data Hardening for On-the-Fly Hardware Error Detection during Database Query Processing"

eval/run.sh COMPILE
./generate_ssbdata.sh || exit 1

pushd eval
source run.conf
./run.sh || (popd && exit 1)
# TODO cp eval/DATE/
popd

pushd paper
pdflatex sigmod2018.tex || (popd && exit 1)
popd

