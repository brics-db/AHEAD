#!/bin/bash

CB_SUBMODULE=coding_benchmark
CB_GITREPO=https://github.com/brics-db/coding_benchmark
CB_BUILDDIR=build

source common.conf

# For the reproducibility, use the submodule coding_benchmark
if [[ -z "$(ls -A ${CB_SUBMODULE})" ]]; then
    git submodule init "${CB_SUBMODULE}"
fi
git submodule update "${CB_SUBMODULE}"

pushd ${CB_SUBMODULE} && ./bootstrap.sh && ./runbench.sh && mv benchmark_novec.out benchmark_novec.err "../${AHEAD_PAPER_RESULTS_MB}" && popd
pushd "../${AHEAD_PAPER_RESULTS_MB}" && gnuplot plot_check_avx2.m && gnuplot plot_decode_avx2.m && gnuplot plot_encode_avx2.m && gnuplot plot_labels.m && popd
