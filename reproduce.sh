#!/bin/bash

# This script is an all-in-one solution to reproduce the results from our SIGMOD 2018 paper titled "AHEAD: Adaptable Data Hardening for On-the-Fly Hardware Error Detection during Database Query Processing"

./generate.sh || exit 1
./bootstrap.sh || exit 1

pushd eval
./run.sh || (popd && exit 1)
popd

pushd paper
pdflatex sigmod2018.tex || (popd && exit 1)
popd

