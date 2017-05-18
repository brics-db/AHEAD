#!/usr/bin/env /bin/bash

rm -Rf ./build

g++ -std=c++14 -O3 -march=native -o createshufflemaskarrays createshufflemaskarrays.cpp &>createshufflemaskarrays.build &
g++ -std=c++14 -O3 -march=native -DNPROCS=$(nproc) -o testshufflemask testshufflemask.cpp ../util/stopwatch.cpp SSE.cpp -I../../include -I.. -fopenmp &>testshufflemask.build &
g++ -std=c++14 -O2 -g3 -march=native -DNPROCS=$(nproc) -o testshufflemask_dbg testshufflemask.cpp ../util/stopwatch.cpp SSE.cpp -I../../include -I.. -fopenmp &>testshufflemask_dbg.build &

wait $(jobs -p)

if [[ -e createshufflemaskarrays.build ]] && [[ -s createshufflemaskarrays.build ]]; then
	echo "createshufflemaskarrays:"
	cat createshufflemaskarrays.build
	echo
fi

if [[ -e testshufflemask.build ]] && [[ -s testshufflemask.build ]]; then
	echo "testshufflemask:"
	cat createshufflemaskarrays.build
	echo
fi

if [[ -e testshufflemask_dbg.build ]] && [[ -s testshufflemask_dbg.build ]]; then
	echo "testshufflemask_dbg:"
	cat createshufflemaskarrays.build
	echo
fi

#g++ -std=c++14 -O3 -march=native -o testshufflemasktable testshufflemasktable.cpp ../util/stopwatch.cpp SSE.cpp -I../../include -I..
