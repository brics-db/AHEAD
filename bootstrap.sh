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

BUILD_BASE_DIR=./build
BUILD_DEBUG_DIR=${BUILD_BASE_DIR}/Debug
BUILD_RELEASE_DIR=${BUILD_BASE_DIR}/Release

if [ -z ${DEBUG+x} ]; then
	debugmode=0
else
	debugmode=1
fi

if [[ ${debugmode} -eq 1 ]]; then
	[[ -e ${BUILD_DEBUG_DIR} ]] && rm -Rf ${BUILD_DEBUG_DIR}
	mkdir -p ${BUILD_DEBUG_DIR} && pushd ${BUILD_DEBUG_DIR} && cmake ../.. -DCMAKE_BUILD_TYPE=Debug || popd
fi

[[ -e ${BUILD_RELEASE_DIR} ]] && rm -Rf ${BUILD_RELEASE_DIR}
mkdir -p ${BUILD_RELEASE_DIR} && pushd ${BUILD_RELEASE_DIR} && cmake ../.. -DCMAKE_BUILD_TYPE=Release || popd
