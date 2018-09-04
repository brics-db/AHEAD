#!/bin/bash

CB_SUBMODULE=coding_benchmark
CB_GITREPO=https://github.com/brics-db/coding_benchmark
CB_BUILDDIR=build

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)/common.conf"

if [[ -z ${reproscript+x} ]]; then
	echo "###########################################################"
	echo "# For the following tests, for better reproducibilty, we: #"
	echo "#   * DISABLE turboboost                                  #"
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
	modes=($(sudo $(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)/scalinggovernor.sh avail 0))
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
	echo "###########################################################"
	echo "# Running Coding Benchmark                                #"
	echo "###########################################################"
	echo
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
