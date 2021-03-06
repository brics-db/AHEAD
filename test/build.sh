#!/usr/bin/env /bin/bash

rm -Rf ./build

#NPROCS=$NPROCS
NPROCS=1

c++ -std=c++17 -O3 -Wno-ignored-attributes -march=native -o createshufflemaskarrays createshufflemaskarrays.cpp &>createshufflemaskarrays.build &
c++ -std=c++17 -O3 -Wno-ignored-attributes -march=native -o createshufflemaskarrays_avx2 createshufflemaskarrays_avx2.cpp &>createshufflemaskarrays_avx2.build &
c++ -std=c++17 -O3 -Wno-ignored-attributes -march=native -DNPROCS=$NPROCS -o testshufflemask testshufflemask.cpp SSE.cpp ../lib/util/stopwatch.cpp -I../include -I../lib -fopenmp &>testshufflemask.build &
c++ -std=c++17 -O2 -Wno-ignored-attributes -g3 -march=native -DNPROCS=$NPROCS -o testshufflemask_dbg testshufflemask.cpp SSE.cpp ../lib/util/stopwatch.cpp -I../include -I../lib -fopenmp &>testshufflemask_dbg.build &
c++ -std=c++17 -O3 -Wno-ignored-attributes -march=native -o functors -I../include functors.cpp &>functors.build &
c++ -std=c++17 -O3 -Wno-ignored-attributes -march=native -o testshufflemask_distributions_native testshufflemask_distributions.cpp SSE.cpp AVX2.cpp ../lib/util/stopwatch.cpp -I../include -I../lib -fopenmp &>testshufflemask_distributions_native.build &
c++ -std=c++17 -O2 -Wno-ignored-attributes -g3 -march=native -o testshufflemask_distributions_native_dbg testshufflemask_distributions.cpp SSE.cpp AVX2.cpp ../lib/util/stopwatch.cpp -I../include -I../lib -fopenmp &>testshufflemask_distributions_native_dbg.build &
c++ -std=c++17 -O3 -Wno-ignored-attributes -mavx2 -o testshufflemask_distributions_avx2 testshufflemask_distributions.cpp SSE.cpp AVX2.cpp ../lib/util/stopwatch.cpp -I../include -I../lib -fopenmp &>testshufflemask_distributions_avx2.build &
c++ -std=c++17 -O2 -Wno-ignored-attributes -g3 -mavx2 -o testshufflemask_distributions_avx2_dbg testshufflemask_distributions.cpp SSE.cpp AVX2.cpp ../lib/util/stopwatch.cpp -I../include -I../lib -fopenmp &>testshufflemask_distributions_avx2_dbg.build &
c++ -std=c++17 -O3 -Wno-ignored-attributes -march=knl -o testshufflemask_distributions_knl testshufflemask_distributions.cpp SSE.cpp AVX2.cpp ../lib/util/stopwatch.cpp -I../include &>testshufflemask_distributions_knl.build &
c++ -std=c++17 -O2 -Wno-ignored-attributes -g3 -march=knl -o testshufflemask_distributions_knl_dbg testshufflemask_distributions.cpp SSE.cpp AVX2.cpp ../lib/util/stopwatch.cpp -I../include &>testshufflemask_distributions_dbg_knl.build &

wait $(jobs -p)

function check {
	if [[ -e $1.build ]] && [[ -s $1.build ]]; then
		echo "$1:"
		cat $1.build
		echo
	fi
}

check createshufflemaskarrays
check createshufflemaskarrays_avx2
check testshufflemask
check testshufflemask_dbg
check functors
check testshufflemask_distributions_native
check testshufflemask_distributions_native_dbg
check testshufflemask_distributions_avx2
check testshufflemask_distributions_avx2_dbg
check testshufflemask_distributions_knl
check testshufflemask_distributions_knl_dbg

#g++ -std=c++14 -O3 -march=native -o testshufflemasktable testshufflemasktable.cpp ../util/stopwatch.cpp SSE.cpp -I../../include -I..

