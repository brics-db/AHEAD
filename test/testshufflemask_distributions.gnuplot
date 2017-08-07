reset
#set term png enhanced
set term cairolatex pdf color fontscale 0.44 size 6.5in, 1.8in dashlength 0.2
infile="testshufflemask_distributions_alienware_2017-08-07.out"
set datafile separator ','
set xtics mirror out
set ytics mirror out
set xrange [-0.1:1.1]

set output "testshufflemask_distributions_8bit.tex"
set key below
set title '8-bit data SSE4.2'
plot infile index 0 using 1:2 ls 1 title column, \
	'' index 0 using 1:3 ls 2 title column

set output "testshufflemask_distributions_16bit.tex"
set key below
set title '16-bit data SSE4.2'
plot infile index 1 using 1:2 ls 1 title column, \
	'' index 1 using 1:3 ls 2 title column

set output "testshufflemask_distributions_32bit.tex"
set key below
set title '32-bit data SSE4.2'
plot infile index 2 using 1:2 ls 1 title column, \
	'' index 2 using 1:3 ls 2 title column

set output "testshufflemask_distributions_64bit.tex"
set key below
set title '64-bit data SSE4.2'
plot infile index 3 using 1:2 ls 1 title column, \
	'' index 3 using 1:3 ls 2 title column

