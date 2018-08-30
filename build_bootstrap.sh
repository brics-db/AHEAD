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

if [[ ${debugmode} -eq 1 ]]; then
	mkdir -p ${AHEAD_BUILD_DEBUG_DIR}
	pushd ${AHEAD_BUILD_DEBUG_DIR} && cmake ../.. -DCMAKE_BUILD_TYPE=Debug || popd
fi

mkdir -p ${AHEAD_BUILD_RELEASE_DIR}
pushd ${AHEAD_BUILD_RELEASE_DIR} && cmake ../.. -DCMAKE_BUILD_TYPE=Release || popd
