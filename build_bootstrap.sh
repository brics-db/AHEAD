#!/usr/bin/env /bin/bash
#
# Copyright (c) 2017 Till Kolditz
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

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd )"
source "${SOURCE_DIR}/common.conf"

if [[ -z ${DEBUG+x} ]]; then
	debugmode=0
else
	debugmode=1
fi

echo " * Bootstrapping AHEAD sources"

if [[ ${debugmode} -eq 1 ]]; then
	echo -n "   * Debug..."
	mkdir -p ${AHEAD_BUILD_DEBUG_DIR} || AHEAD_quit 1 " Error creating directory '${AHEAD_BUILD_DEBUG_DIR}'!"
	pushd ${AHEAD_BUILD_DEBUG_DIR} &>/dev/null || AHEAD_quit 1 " Error accessing directory '${AHEAD_BUILD_DEBUG_DIR}'!"
	AHEAD_run_hidden_output "cmake" "../.." "-DCMAKE_BUILD_TYPE=Debug" || exit 1
fi

echo -n "   * Release..."
mkdir -p ${AHEAD_BUILD_RELEASE_DIR} || AHEAD_quit 1 " Error creating directory '${AHEAD_BUILD_RELEASE_DIR}'!"
pushd ${AHEAD_BUILD_RELEASE_DIR} &>/dev/null || AHEAD_quit 1 " Error accessing directory '${AHEAD_BUILD_RELEASE_DIR}'!"
AHEAD_run_hidden_output "cmake" "../.." "-DCMAKE_BUILD_TYPE=Release" || exit 1
