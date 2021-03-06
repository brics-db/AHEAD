#!/usr/bin/env bash

# Copyright (c) 2016-2017 Till Kolditz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

for f in ssbm-q11 ssbm-q11_eager ssbm-q11_lazy ssbm-q11_encoded; do
	grep -o 'result.*$' ${f}.out >${f}.result
	grep -A 10 "TotalTimes" ${f}.out >${f}.out.summary
done
#grep -o 'result.*$' ssbm-q01.out >ssbm-q01.result
#grep -o 'result.*$' ssbm-q01_eager.out >ssbm-q01_eager.result
#grep -o 'result.*$' ssbm-q01_lazy.out >ssbm-q01_lazy.result

DIFF1=$(if [[ $(diff ssbm-q01.result ssbm-q01_eager.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
DIFF2=$(if [[ $(diff ssbm-q01.result ssbm-q01_lazy.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
DIFF3=$(if [[ $(diff ssbm-q01.result ssbm-q01_encoded.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
DIFF4=$(if [[ $(diff ssbm-q01_lazy.result ssbm-q01_eager.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
DIFF5=$(if [[ $(diff ssbm-q01_lazy.result ssbm-q01_encoded.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)
DIFF6=$(if [[ $(diff ssbm-q01_eager.result ssbm-q01_encoded.result | wc -l) -eq 0 ]]; then echo -n "OK"; else echo -n "FAIL"; fi)

if [[ DIFF1 -eq "OK" ]] && [[ DIFF2 -eq "OK" ]] && [[ DIFF3 -eq "OK" ]] && [[ DIFF4 -eq "OK" ]] && [[ DIFF5 -eq "OK" ]] && [[ DIFF6 -eq "OK" ]]; then
	echo "all OK"
	exit 0;
fi

if [[ DIFF1 -eq "OK" ]]; then
	if [[ DIFF2 -eq "OK" ]]; then
		echo "All produce the same result"
		exit 0
	else
		echo "Only ssbm-q01 and ssbm-q01_eager produce the same result"
		exit 1
	fi
else
	if [[ DIFF2 -eq "OK" ]]; then
		echo "Only ssbm-q01 and ssbm-q01_lazy produce the same result"
		exit 2
	elif [[ DIFF3 -eq "OK" ]]; then
		echo "Only ssbm-q01_lazy and ssbm-q01_eager produce the same result"
		exit 3
	fi
fi

#echo "" >lineorder_size.out; for sf in `seq 1 10`; do date +%T | tr -d '\n'; echo -n ": Scale Factor ${sf}... "; echo "Scale Factor ${sf}" >>lineorder_size.out; ./lineorder_size ../../database/sf-${sf} >>lineorder_size.out; date +%T | tr -d '\n'; echo ": Done!"; done

