#!/usr/bin/env gnuplot
#set term pdf enhanced color fontscale 0.44 size 1.6in,1.25in
#set output 'minbfw_consumption.pdf'
set term cairolatex pdf color fontscale 0.44 size 1.7in,1in dashlength 0.2
set output 'minbfw_consumption.tex'
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
set ylabel "Relative Consumption" offset 0.5,-2
set label 1 "1.43" rotate by 90 front at first 0.5,1.7
set label 2 "1.49" rotate by 90 front at first 1.5,1.7
set label 3 "1.55" rotate by 90 front at first 2.5,1.7
set label 4 "1.61" rotate by 90 front at first 3.5,1.7
plot 'minbfw_consumption.data' using 2 title col fillstyle pattern 0 border ls 1 lw 1 dt 1, \
         '' using 3 title col fillstyle pattern 2 border ls 2 lw 1 dt 1, \
         '' using 4 title col fillstyle pattern 3 border ls 4 lw 1 dt 1

set term pngcairo enhanced notransparent size 10in,5in dashlength 0.2
set output 'minbfw_consumption.png'
set tmargin
set rmargin
set lmargin
set bmargin 6
set key below horizontal
set format y
unset label
set xtics ("1" 0, "2" 1, "3" 2, "4{^*}" 3) nomirror out
set label 2 "1.49" rotate by 45 front at first 1.2,1.7
set label 3 "1.55" rotate by 45 front at first 2.2,1.7
set label 4 "1.61" rotate by 45 front at first 3.2,1.7
set label 1 "1.43" rotate by 45 front at first 0.2,1.7
set xlabel "Minimum Detectable Bit Flip Weight"
plot 'minbfw_consumption.data' using 2 fillstyle pattern 3 border ls 1 lw 1 dt 1 title col, \
         '' using 3 fillstyle pattern 2 border ls 2 lw 1 dt 1 title col, \
         '' using 4 fillstyle pattern 6 border ls 6 lw 1 dt 1 title col