#!/usr/bin/env gnuplot

set datafile separator '\t'
set style data histogram
set style histogram cluster gap 0
set style fill pattern 0 border
set style line 1 lc rgb "#9400d3"
set style line 2 lc rgb "#009e73"
set style line 3 lc rgb "#56b4e9"
set style line 4 lc rgb "#e69f00"
set style line 5 lc rgb "#000000"
set style line 6 lc rgb "#0072b2"
set style line 7 lc rgb "#e51e10"

set term cairolatex pdf color fontscale 0.44 size 1in,1in dashlength 0.2
set output 'consumption.tex'
unset key
set boxwidth 0.75
set auto x
unset xtics
set yrange [0:2.5]
set grid noxtics noytics
set ylabel "Relative Memory\nConsumption" offset 1,0
unset xlabel
set tmargin 0.5
set rmargin 0.5
set bmargin 1
set lmargin 8
set format y "\\num{%g}"
set label "1" at first -0.325,1.25
set label "2.00" at first 0,2.25
set label "1.50" at first 0.4,1.75
plot 'consumption.data' using 2:xtic(1) title col fillstyle pattern 0 border ls 1 lw 1 dt 1, \
         '' using 3:xtic(1) title col fillstyle pattern 2 border ls 2 lw 1 dt 1, \
         '' using 6:xtic(1) title col fillstyle pattern 1 border ls 6 lw 1 dt 1

set term pngcairo enhanced notransparent size 10in,5in dashlength 0.2
set output 'consumption.png'
set tmargin
set rmargin
set lmargin
set bmargin 4
set key below horizontal
set format y
plot 'consumption.data' using 2:xtic(1) fillstyle pattern 3 border ls 1 lw 1 dt 1 title col, \
         '' using 3:xtic(1) fillstyle pattern 2 border ls 2 lw 1 dt 1 title col, \
         '' using 6:xtic(1) fillstyle pattern 6 border ls 6 lw 1 dt 1 title col





reset

set datafile separator '\t'
set style data histogram
set style histogram cluster gap 0
set style fill pattern 0 border
set style line 1 lc rgb "#9400d3"
set style line 2 lc rgb "#009e73"
set style line 3 lc rgb "#56b4e9"
set style line 4 lc rgb "#e69f00"
set style line 5 lc rgb "#000000"
set style line 6 lc rgb "#0072b2"
set style line 7 lc rgb "#e51e10"
set style line 99 lc rgb "#ffffff"

set term pngcairo enhanced notransparent font "Muli,14" size 3in,4in
unset key
set boxwidth 0.75
set auto x
unset xtics
set yrange [0:2.5]
set grid noxtics noytics
set y2label "Relative Memory\nConsumption" offset 1,0
unset xlabel
set border 1
unset ytics
set output 'consumption1.png'
set label "1" at first -0.325,1.25
plot 'consumption.data' using 2:xtic(1) fillstyle pattern 3 border ls 1 lw 1 dt 1 title col, \
         '' using 3:xtic(1) fillstyle pattern 2 border ls 99 lw 1 dt 1 title col, \
         '' using 6:xtic(1) fillstyle pattern 6 border ls 99 lw 1 dt 1 title col
unset output
set output 'consumption2.png'
set label "2.00" at first 0,2.25
plot 'consumption.data' using 2:xtic(1) fillstyle pattern 3 border ls 1 lw 1 dt 1 title col, \
         '' using 3:xtic(1) fillstyle pattern 2 border ls 2 lw 1 dt 1 title col, \
         '' using 6:xtic(1) fillstyle pattern 6 border ls 99 lw 1 dt 1 title col
unset output
set output 'consumption3.png'
set label "1.50" at first 0.4,1.75
plot 'consumption.data' using 2:xtic(1) fillstyle pattern 3 border ls 1 lw 1 dt 1 title col, \
         '' using 3:xtic(1) fillstyle pattern 2 border ls 2 lw 1 dt 1 title col, \
         '' using 6:xtic(1) fillstyle pattern 6 border ls 6 lw 1 dt 1 title col
unset output