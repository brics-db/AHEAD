reset
set term cairolatex pdf color fontscale 0.44 size 2.6in, 2in dashlength 0.2
infile=
set datafile separator ','
set xtics mirror out
set ytics mirror out
set xrange [-0.1:1.1]
set yrange [0:*]
set bmargin 5
unset key
unset title
unset ylabel
set xlabel "Selectivity" offset 0,-0.5

set output "testshufflemask_distributions_sse_8bit.tex"
plot infile index 0 using 1:2 ls 1 title column, \
	'' index 0 using 1:3 ls 2 title column

set output "testshufflemask_distributions_sse_16bit.tex"
plot infile index 1 using 1:2 ls 1 title column, \
	'' index 1 using 1:3 ls 2 title column

set output "testshufflemask_distributions_sse_32bit.tex"
plot infile index 2 using 1:2 ls 1 title column, \
	'' index 2 using 1:3 ls 2 title column

set output "testshufflemask_distributions_sse_64bit.tex"
plot infile index 3 using 1:2 ls 1 title column, \
	'' index 3 using 1:3 ls 2 title column

set output "testshufflemask_distributions_avx2_8bit.tex"
plot infile index 4 using 1:2 ls 1 title column, \
	'' index 4 using 1:3 ls 2 title column

set output "testshufflemask_distributions_avx2_16bit.tex"
plot infile index 5 using 1:2 ls 1 title column, \
	'' index 5 using 1:3 ls 2 title column

set output "testshufflemask_distributions_avx2_32bit.tex"
plot infile index 6 using 1:2 ls 1 title column, \
	'' index 6 using 1:3 ls 2 title column

set output "testshufflemask_distributions_avx2_64bit.tex"
plot infile index 7 using 1:2 ls 1 title column, \
	'' index 7 using 1:3 ls 2 title column

set term cairolatex pdf color fontscale 0.44 size .2in, 2in dashlength 0.2
set output 'ylabel.tex'
set bmargin 0
set lmargin 0
set rmargin 0
set tmargin 0
set noborder
set noxtics
set noytics
set noxlabel
set noylabel
set notitle
unset xlabel
set ylabel "Runtime [ns]"
set yrange [-1:0]
set xrange [-1:0]
set label 'Runtime [s]' at screen 0.5, 0.5 offset 0,-strlen("Runtime [s]")/4.0 rotate by 90
plot 1/0 ls 0 t ""

set term cairolatex pdf color fontscale 0.44 size 2.6in, .2in dashlength 0.2
set output 'legend.tex'
set key below width 5
plot 1/0 ls 1 t "Lookup-Table" with points,\
	1/0 ls 2 t "Sequential" with points
