#!/usr/bin/env /bin/bash

g++ -std=c++14 -O3 -o createshufflemaskarrays createshufflemaskarrays.cpp

g++ -DNPROCS=$(nproc) -std=c++14 -O3 -march=native -o testshufflemask testshufflemask.cpp ../util/stopwatch.cpp SSE.cpp -I../../include -I.. -fopenmp
g++ -DNPROCS=$(nproc) -std=c++14 -O2 -g3 -march=native -o testshufflemask_dbg testshufflemask.cpp ../util/stopwatch.cpp SSE.cpp -I../../include -I.. -fopenmp

#g++ -std=c++14 -O3 -march=native -o testshufflemasktable testshufflemasktable.cpp ../util/stopwatch.cpp SSE.cpp -I../../include -I..
