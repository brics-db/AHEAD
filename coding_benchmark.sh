#!/bin/bash

CB_SUBMODULE=coding_benchmark
CB_GITREPO=https://github.com/brics-db/coding_benchmark
CB_BUILDDIR=build

source common.conf

# For the reproducibility, use the submodule coding_benchmark
if [[ -z "$(ls -A ${CB_SUBMODULE})" ]]; then
    git submodule init --recursive "${CB_SUBMODULE}"
fi
git submodule update --recursive "${CB_SUBMODULE}"

pushd ${CB_SUBMODULE} && ./bootstrap.sh && pushd "${CB_BUILDDIR}/Release" && make -j$(nproc) && ./benchmark16 1> >(tee benchmark.out) 2> >(tee benchmark.err >&2) && popd && mv "${CB_BUILDDIR}/Release/benchmark.out" "${CB_BUILDDIR}/Release/benchmark.err" "../${AHEAD_PAPER_RESULTS_MB}" && popd && pushd "../${AHEAD_PAPER_RESULTS_MB}" && gnuplot plot_check_avx2.m && gnuplot plot_decode_avx2.m && gnuplot plot_encode_avx2.m && gnuplot plot_labels.m && popd
