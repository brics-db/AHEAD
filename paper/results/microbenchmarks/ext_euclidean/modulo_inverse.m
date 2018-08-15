#!/usr/bin/env gnuplot

# The data was gained by
# ./TestModuloInverseComputation2 10000 2 127

infile='TestModuloInverseComputation2.data'
set datafile separator '\t'

pps=0
llw=2
set style line 1 lc rgb "#9400d3" lw llw ps pps
set style line 2 lc rgb "#009e73" lw llw ps pps
set style line 3 lc rgb "#56b4e9" lw llw ps pps
set style line 4 lc rgb "#e69f00" lw llw ps pps
set style line 5 lc rgb "#000000" lw llw ps pps
set style line 6 lc rgb "#0072b2" lw llw ps pps
set style line 7 lc rgb "#e51e10" lw llw ps pps

set term cairolatex pdf color fontscale 0.44 size 2.9in,1in dashlength 0.2
set output 'TestModuloInverseComputation2.tex'
set style line 99 lc rgb "#aaaaaa" lt 1 lw 1
set grid xtics ytics back ls 99
set xrange [3:127]
set offsets 3, 3, 0, 0
#set xtics rotate by 45 offset -1,-1 (3,10,20,30,40,50,60,70,80,90,100,110,120)
set xtics rotate by 45 offset -1,-1 (" 7" 7, 15, 31, 63, 127)
set yrange [0:*]
set xlabel "$|C|$ [\\# bits]"
set ylabel "Compute Time [ns]"
set format y "\\num{%g}"
set key right outside center vertical samplen 1 spacing 1.4
#plot infile using 1:4  index 1 ls 1 w lines t "$|C|\\lt7$", \
#         '' using 1:7  index 2 ls 2 w lines t "$|C|\\lt15$", \
#         '' using 1:10 index 3 ls 3 w lines t "$|C|\\lt31$", \
#         '' using 1:10 index 4 ls 3 w lines t "$|C|\\lt63$", \
#         '' using 1:13 index 5 ls 4 w lines t "$|C|\\lt127$"

plot for [i=2:6] infile using 1:i t col w linespoints ls i
