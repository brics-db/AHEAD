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

#for sf in 1; do
for type in ssbm-q11 ssbm-q11_eager ssbm-q11_lazy ssbm-q11_encoded; do
	FILEOUT="${type}.out"
	FILEERR="${type}.err"
	echo "" >${FILEOUT}
	echo "" >${FILEERR}
	for sf in `seq 1 10`; do
		./${type} --dbpath ../../database/sf-${sf} 1>>${FILEOUT} 2>>${FILEERR}
#		./${type} --dbpath ../../database/sf-${sf}
	done
done

#echo "" >lineorder_size.out
#for sf in `seq 1 10`; do
#	./lineorder_size ../../database/sf-${sf} | tee -a lineorder_size.out
#done

