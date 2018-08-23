#!/usr/bin/env gnuplot
#set term pdf enhanced color fontscale 0.44 size 2.9in,1.25in
#set output 'vectorVSscalar1_1-1_3.pdf'
set term cairolatex pdf color fontscale 0.44 size 2.9in,1in dashlength 0.2
set output 'vectorVSscalar1_1-1_3.tex'
set style data histogram
set style histogram cluster gap 1
set auto x
#set key vertical right outside
set key below horizontal
set style line 1 lc rgb "#9400d3"
set style line 2 lc rgb "#009e73"
set style line 3 lc rgb "#56b4e9"
set style line 4 lc rgb "#e69f00"
set style line 5 lc rgb "#000000"
set style line 6 lc rgb "#0072b2"
set style line 7 lc rgb "#e51e10"
set yrange [0.9:]
set ytics mirror out
set xtics nomirror out
set grid noxtics ytics
unset xlabel
set ylabel "Relative Runtime"
set yrange [0:3]
set label "5.10" at first 0.5, screen 0.97
set label "5.14" at first 1.5, screen 0.97
set arrow from first -0.15,2.50 to first 0.15,1.00 head filled front
set label "2.6" at first 0.2,2.00 rotate by -75
set arrow from first 0.85,3 to first 1.15,2.00 head filled front
set label "2.5" at first 1.2,2.80 rotate by -70
set arrow from first 1.85,3 to first 2.15,1.47 head filled front
set label "3.5" at first 2.2,2.80 rotate by -70
set arrow from first 2.85,2.63 to first 3.15,1.05 head filled front
set label "2.5" at first 3.2,2.20 rotate by -70
set arrow from first 3.85,2.89 to first 4.15,1.26 head filled front
set label "2.3" at first 4.2,2.50 rotate by -70
set arrow from first 4.85,2.93 to first 5.15,1.29 head filled front
set label "2.3" at first 5.2,2.50 rotate by -70
plot 'vectorVSscalar1_1-1_3.data' using 2:xtic(1) title col fillstyle pattern 0 border ls 1 lw 1 dt 1, \
         '' using 3:xtic(1) title col fillstyle pattern 2 border ls 2 lw 1 dt 1

set term pngcairo enhanced notransparent size 10in,5in dashlength 0.2
set output 'vectorVSscalar1_1-1_3.png'
set bmargin 4
plot 'vectorVSscalar1_1-1_3.data' using 2:xtic(1) title col fillstyle pattern 3 border ls 3 lw 1 dt 1, \
         '' using 3:xtic(1) title col fillstyle pattern 1 border ls 1 lw 1 dt 1