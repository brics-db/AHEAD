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

set margin 0
set noborder
unset tics
set notitle
set xrange [-20:-10]
set yrange [-20:-10]

set terminal cairolatex pdf input blacktext color transparent size .2in,1.25in
set output 'plot_paper_ylabel_scalar.tex'
set label 'Scalar Runtime [s]' at screen 0.6,0.5 offset 0,-strlen("Scalar Runtime [s]")/10.0 rotate by 90
plot 20 ls 0 with linespoints

unset label

set output 'plot_paper_ylabel_vectorized.tex'
set label 'SSE Runtime [s]' at screen 0.6,0.5 offset 0,-strlen("SSE Runtime [s]")/10.0 rotate by 90
plot 20 ls 0 with linespoints

unset label
set key below spacing 1.5

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

set terminal cairolatex pdf input blacktext color transparent size 6.5in,.4in
set output 'plot_paper_legend.tex'
set margin 0
plot 20 ls 1 t "XOR$^{\\text{scalar/SSE}}$" with linespoints,\
	20 ls 6 t "XOR$^{\\text{AVX2}}$" with linespoints,\
	20 ls 4 t "AN$_{U}^{\\text{scalar/SSE}}$" with linespoints,\
	20 ls 5 t "AN$_{S}^{\\text{scalar/SSE}}$" with linespoints,\
	20 ls 7 t "AN$_{U}^{\\text{AVX2}}$" with linespoints,\
	20 ls 8 t "AN$_{S}^{\\text{AVX2}}$" with linespoints,\
	20 ls 3 t "Hamming" with linespoints
