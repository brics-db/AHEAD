#!/usr/bin/env /bin/bash

rm -Rf ./build

#NPROCS=$NPROCS
NPROCS=1

c++ -std=c++14 -O3 -march=native -o createshufflemaskarrays createshufflemaskarrays.cpp &>createshufflemaskarrays.build &
c++ -std=c++14 -O3 -march=native -o createshufflemaskarrays_avx2 createshufflemaskarrays_avx2.cpp &>createshufflemaskarrays_avx2.build &
c++ -std=c++14 -O3 -march=native -DNPROCS=$NPROCS -o testshufflemask testshufflemask.cpp SSE.cpp ../lib/util/stopwatch.cpp -I../include -I../lib -fopenmp &>testshufflemask.build &
c++ -std=c++14 -O2 -g3 -march=native -DNPROCS=$NPROCS -o testshufflemask_dbg testshufflemask.cpp SSE.cpp ../lib/util/stopwatch.cpp -I../include -I../lib -fopenmp &>testshufflemask_dbg.build &
c++ -std=c++14 -O3 -march=native -o functors -I../include functors.cpp &>functors.build &
c++ -std=c++14 -O3 -march=native -DNPROCS=$NPROCS -o testshufflemask_distributions testshufflemask_distributions.cpp SSE.cpp AVX2.cpp ../lib/util/stopwatch.cpp -I../include -I../lib -fopenmp &>testshufflemask_distributions.build &
c++ -std=c++14 -O2 -g3 -march=native -DNPROCS=$NPROCS -o testshufflemask_distributions_dbg testshufflemask_distributions.cpp SSE.cpp AVX2.cpp ../lib/util/stopwatch.cpp -I../include -I../lib -fopenmp &>testshufflemask_distributions_dbg.build &

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
check testshufflemask_distributions
check testshufflemask_distributions_dbg

#g++ -std=c++14 -O3 -march=native -o testshufflemasktable testshufflemasktable.cpp ../util/stopwatch.cpp SSE.cpp -I../../include -I..

