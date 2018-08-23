#!/usr/bin/env gnuplot
#set term pdf enhanced color fontscale 0.44 size 1.4in,1.25in
#set output 'minbfw_runtime.pdf'
set term cairolatex pdf color fontscale 0.44 size 1.4in,1in dashlength 0.2
set output 'minbfw_runtime.tex'
set style data histogram
set style histogram cluster gap 1
set auto x
#set key vertical right outside
set key below horizontal samplen 2
set style line 1 lc rgb "#9400d3"
set style line 2 lc rgb "#009e73"
set style line 3 lc rgb "#56b4e9"
set style line 4 lc rgb "#e69f00"
set style line 5 lc rgb "#000000"
set style line 6 lc rgb "#0072b2"
set style line 7 lc rgb "#e51e10"
set ytics nomirror out
set xtics ("1" 0, "2" 1, "3" 2, "4$^*$" 3) nomirror out
set rmargin 0
set tmargin 0.3
set grid noxtics ytics
unset xlabel
set yrange [0:*]
set ylabel "Relative Runtime" offset 0.5,-1
plot 'minbfw_runtime.data' using 2 title col fillstyle pattern 0 border ls 1 lw 1 dt 1, \
         '' using 3 title col fillstyle pattern 2 border ls 2 lw 1 dt 1
