#!/bin/bash

CB_SUBMODULE=coding_benchmark
CB_GITREPO=https://github.com/brics-db/coding_benchmark
CB_BUILDDIR=build

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)/common.conf"

AHEAD_prepare_scalinggovernor_and_turboboost

# For the reproducibility, use the submodule coding_benchmark
if [[ -z "$(ls -A ${CB_SUBMODULE})" ]]; then
    git submodule init --recursive "${CB_SUBMODULE}"
fi
git submodule update --recursive "${CB_SUBMODULE}"

# Build, run, and copy the codign_benchmark results
pushd ${CB_SUBMODULE} && ./bootstrap.sh && pushd "${CB_BUILDDIR}/Release" && make -j$(nproc) || exit 1
./benchmark16 1> >(tee benchmark.out) 2> >(tee benchmark.err >&2) && popd && mv "${CB_BUILDDIR}/Release/benchmark.out" "${CB_BUILDDIR}/Release/benchmark.err" "../${AHEAD_PAPER_RESULTS_MB}"
popd
