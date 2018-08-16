#!/bin/bash

CB_SUBMODULE=coding_benchmark
CB_GITREPO=https://github.com/brics-db/coding_benchmark
CB_BUILDDIR=build

source common.conf

if [[ -z ${reproscipt+x} ]]; then
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
fi

# For the reproducibility, use the submodule coding_benchmark
if [[ -z "$(ls -A ${CB_SUBMODULE})" ]]; then
    git submodule init --recursive "${CB_SUBMODULE}"
fi
git submodule update --recursive "${CB_SUBMODULE}"

# Build, run, and copy the codign_benchmark results
pushd ${CB_SUBMODULE} && ./bootstrap.sh && pushd "${CB_BUILDDIR}/Release" && make -j$(nproc) || exit 1
./benchmark16 1> >(tee benchmark.out) 2> >(tee benchmark.err >&2) && popd && mv "${CB_BUILDDIR}/Release/benchmark.out" "${CB_BUILDDIR}/Release/benchmark.err" "../${AHEAD_PAPER_RESULTS_MB}"
popd

# gnuplot the results
pushd "${AHEAD_PAPER_RESULTS_MB}" && gnuplot plot_check_avx2.m && gnuplot plot_decode_avx2.m && gnuplot plot_encode_avx2.m && gnuplot plot_labels.m
popd
