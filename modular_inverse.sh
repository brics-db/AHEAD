#!/bin/bash

MI_SUBMODULE=coding_benchmark
MI_GITREPO=https://github.com/brics-db/coding_benchmark
MI_BUILDDIR=build
MI_EXEC="TestModuloInverseComputation2"
MI_OUTFILE="${MI_EXEC}.out"
MI_ERRFILE="${MI_EXEC}.err"
MI_NUMRUNS=10000
MI_MIN=2
MI_MAX=127

source common.conf

# For the reproducibility, use the submodule coding_benchmark
if [[ -z "$(ls -A ${MI_SUBMODULE})" ]]; then
    git submodule init "${MI_SUBMODULE}"
fi
git submodule update "${MI_SUBMODULE}"

pushd ${MI_SUBMODULE}/${MI_BUILDDIR}/Release && ./${MI_EXEC} ${MI_NUMRUNS} ${MI_MIN} ${MI_MAX} 1> >(tee ${MI_OUTFILE}) 2> >(tee ${MI_ERRFILE} >&2) && mv ${MI_OUTFILE} ${MI_ERRFILE} "../${AHEAD_PAPER_RESULTS_MI}" && popd
pushd "../${AHEAD_PAPER_RESULTS_MI}" && gnuplot modulo_inverse.m && popd
