#/usr/bin/env /bin/bash

echo "Verifying previous runs of R-Store..."

for q in 11 12 13 21; do
	for t in early late dmr_seq dmr_mt continuous continuous_reenc; do
		for s in seq SSE; do 
			rm -f /tmp/grep.out
			diff ssbm-q${q}_normal_${s}.out ssbm-q${q}_${t}_${s}.out | grep result >/tmp/grep.out
			if [[ -s /tmp/grep.out ]]; then
				echo "Q${q} ${t} ${s} (OUT):"
				cat /tmp/grep.out
				echo "-------------------------------"
			fi
			rm -f /tmp/grep.out
			diff ssbm-q${q}_normal_${s}.err ssbm-q${q}_${t}_${s}.err | grep result >/tmp/grep.out
			if [[ -s /tmp/grep.out ]]; then
				echo "Q${q} ${t} ${s} (ERR):"
				cat /tmp/grep.out
				echo "-------------------------------"
			fi
		done
	done
done

echo "Done."

