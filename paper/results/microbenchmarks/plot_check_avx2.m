#!/usr/bin/env gnuplot

# Copyright 2016,2017 Till Kolditz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,\
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

###############
# Paper-plots #
###############
reset
infile='benchmark.out'
set terminal cairolatex pdf input blacktext color fontscale 0.44 transparent size 1.3in,1.25in
set datafile separator ','
set log x 2
set xrange [1:1024]
set format x "$2^{%L}$"
set log x2 2
set x2range [1:1024]
set xtics 1,4,1024
set xlabel "unroll / block size"
set ytics autofreq
set yrange [*:*]
unset ylabel
set border 1+2+4+8
unset key
set tmargin 1
set rmargin 1
set bmargin 3
set lmargin 4

set style line 1 lt 1 lw 2 ps 0.5
set style line 2 lt 2 lw 2 ps 0.5
set style line 3 lt 3 lw 2 ps 0.5
set style line 4 lt 4 lw 2 ps 0.5
set style line 5 lt 5 lw 2 ps 0.5
set style line 6 lt 6 lw 2 ps 0.5
set style line 7 lt 7 lw 2 ps 0.5
set style line 8 lt 8 lw 2 ps 0.5
set style line 9 lt 9 lw 2 ps 0.5

# 1 XOR      seq / SSE4.2
# 3 Hamming  seq / SSE4.2
# 4 AN inv U seq / SSE4.2
# 5 AN inv S seq / SSE4.2
# 6 XOR      AVX2
# 7 AN inv U AVX2
# 8 AN inv S AVX2

set output 'plot_paper_check_16bit_seq.tex'
plot infile index 0 using 1:21 ls 1 with linespoints,\
	'' index 0 using 1:22 ls 4 with linespoints,\
	'' index 0 using 1:23 ls 5 with linespoints,\
	'' index 0 using 1:26 ls 3 with linespoints

set output 'plot_paper_check_16bit_seq_inv.tex'
plot infile index 0 using 1:21 ls 1 with linespoints,\
	'' index 0 using 1:24 ls 4 with linespoints,\
	'' index 0 using 1:25 ls 5 with linespoints

set output 'plot_paper_check_16bit_vec.tex'
plot infile index 0 using 1:27 ls 1 with linespoints,\
	'' index 0 using 1:28 ls 4 with linespoints,\
	'' index 0 using 1:29 ls 5 with linespoints,\
	'' index 0 using 1:32 ls 3 with linespoints,\
	'' index 0 using 1:33 ls 6 with linespoints,\
	'' index 0 using 1:34 ls 7 with linespoints,\
	'' index 0 using 1:35 ls 8 with linespoints

set output 'plot_paper_check_16bit_vec_inv.tex'
plot infile index 0 using 1:27 ls 1 with linespoints,\
	'' index 0 using 1:30 ls 4 with linespoints,\
	'' index 0 using 1:31 ls 5 with linespoints,\
	'' index 0 using 1:33 ls 6 with linespoints,\
	'' index 0 using 1:36 ls 7 with linespoints,\
	'' index 0 using 1:37 ls 8 with linespoints
