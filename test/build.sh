#!/usr/bin/env /bin/bash

rm -Rf ./build

g++ -std=c++14 -O3 -march=native -o createshufflemaskarrays createshufflemaskarrays.cpp &>createshufflemaskarrays.build &
g++ -std=c++14 -O3 -march=native -DNPROCS=$(nproc) -o testshufflemask testshufflemask.cpp ../lib/util/stopwatch.cpp ../lib/column_operators/SSE.cpp -I../include -I../lib/column_operators -fopenmp &>testshufflemask.build &
g++ -std=c++14 -O2 -g3 -march=native -DNPROCS=$(nproc) -o testshufflemask_dbg testshufflemask.cpp ../lib/util/stopwatch.cpp ../lib/column_operators/SSE.cpp -I../include -I../lib/column_operators -fopenmp &>testshufflemask_dbg.build &
g++ -std=c++14 -O3 -march=native -o functors -I../include functors.cpp &>functors.build &

wait $(jobs -p)

if [[ -e createshufflemaskarrays.build ]] && [[ -s createshufflemaskarrays.build ]]; then
	echo "createshufflemaskarrays:"
	cat createshufflemaskarrays.build
	echo
fi

if [[ -e testshufflemask.build ]] && [[ -s testshufflemask.build ]]; then
	echo "testshufflemask:"
	cat testshufflemask.build
	echo
fi

if [[ -e testshufflemask_dbg.build ]] && [[ -s testshufflemask_dbg.build ]]; then
	echo "testshufflemask_dbg:"
	cat testshufflemask_dbg.build
	echo
fi

if [[ -e functors.build ]] && [[ -s functors.build ]]; then
	echo "functors:"
	cat functors.build
	echo
fi

#g++ -std=c++14 -O3 -march=native -o testshufflemasktable testshufflemasktable.cpp ../util/stopwatch.cpp SSE.cpp -I../../include -I..
