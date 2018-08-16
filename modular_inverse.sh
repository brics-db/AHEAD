#!/bin/bash

MI_SUBMODULE=coding_benchmark
MI_GITREPO=https://github.com/brics-db/coding_benchmark
MI_BUILDDIR=build
MI_EXEC="TestModuloInverseComputation2"
MI_OUTFILE="${MI_EXEC}.out"
MI_ERRFILE="${MI_EXEC}.err"
MI_DATFILE="${MI_EXEC}.data"
MI_SCRIPTFILE="modulo_inverse.m"
MI_NUMRUNS=10000
MI_A_MIN=2
MI_C_MAX=127

source common.conf

# For the reproducibility, use the submodule coding_benchmark
if [[ -z "$(ls -A ${MI_SUBMODULE})" ]]; then
    git submodule init "${MI_SUBMODULE}"
fi
git submodule update "${MI_SUBMODULE}"

basedir="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd )"

pushd ${MI_SUBMODULE}/${MI_BUILDDIR}/Release && ./${MI_EXEC} ${MI_NUMRUNS} ${MI_A_MIN} ${MI_C_MAX} 1> >(tee ${MI_OUTFILE}) 2> >(tee ${MI_ERRFILE} >&2) && mv ${MI_OUTFILE} ${MI_ERRFILE} "${basedir}/${AHEAD_PAPER_RESULTS_MI}" && popd

# In the following, prepare the results for printing.
# The benchmarking tool (TestModuloInverseComputation2) measures for several widhts of A for each possible code word width.
# The runtimes are the TOTAL RUNTIMES for all ${MI_NUMRUNS} !!! Therefore for a single run we need to divide by ${MI_NUMRUNS}
# For the "AHEAD" paper we compute the average of all runtimes per code word width
# When computing the modular inverse, the code word width is restrained -- we need an additional bit for the extended euclidean algorithm.
#   Therefore, for a particular register width, we can only use width-1 wide code words. For instance, using 16-bit registers, we can only compute the modular inverse up to 15-bit wide code words.

pushd "${basedir}/${AHEAD_PAPER_RESULTS_MI}" || exit 1
rm -f ${MI_DATFILE}
echo -n '\(|C|\)' >${MI_DATFILE}
mywidth=8
while [[ ${mywidth} < ${MI_A_MIN} ]]; do
	((mywidth *= 2))
done
mywidths=(${mywidth})
while [[ ${mywidth} -le ${MI_C_MAX} ]]; do
	((mywidth *= 2))
	mywidths+=(${mywidth})
done
widthmax=${mywidths[${#mywidths[@]}-1]}
widthlen=${#widthmax}
for width in "${mywidths[@]}"; do
	echo -ne '\t\(|C|\leq'"$((width-1))"'\)' >>${MI_DATFILE}
done
echo >>${MI_DATFILE}

# The following prepares (and overwrites) the gnuplot file, depending on the parameters with which the benchmark was called (${MI_A_MIN} and ${MI_C_MAX}, see above)
awk --field-separator='\t' 'NR>2{printf "%d",$1; numtabs=1; for (mywidth=8; $1 >= mywidth; mywidth=mywidth*2) {++numtabs}; for(i=0; i<numtabs; ++i) printf "\t"; average=0; for(i=1; i<=NF; ++i) average+=$i; printf "%d\n",((average/'${MI_NUMRUNS}')/(NF-1))}' ${MI_OUTFILE} >>${MI_DATFILE}

cat >${MI_SCRIPTFILE} <<EOF
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
set xrange [$((MI_A_MIN+1)):${MI_C_MAX}]
set offsets 3, 3, 0, 0
set xtics rotate by 45 offset -1,-1 ($(for idx in ${!mywidths[@]}; do if [[ $idx > 0 ]]; then printf ", "; fi; printf "\"%${widthlen}d\" %d" $((${mywidths[$idx]} - 1)) $((${mywidths[$idx]} - 1)); done))
set yrange [0:*]
set xlabel "$|C|$ [\\# bits]"
set ylabel "Compute Time [ns]"
set format y "\\num{%g}"
set key right outside center vertical samplen 1 spacing 1.4

plot for [i=2:$((${#mywidths[@]}+1))] infile using 1:i t col w linespoints ls i
EOF

# Finally, gnuplot! :-)
gnuplot ${MI_SCRIPTFILE} && popd
