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

CMAKE_MINIMUM_REQUIRED(VERSION 3.8 FATAL_ERROR)

IF(POLICY CMP0048)
    CMAKE_POLICY(SET CMP0048 NEW)
    PROJECT(AHEAD VERSION 0.9 LANGUAGES CXX)
ELSE()
    PROJECT(AHEAD LANGUAGES CXX)
ENDIF()

set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
set(CMAKE_DISABLE_SOURCE_CHANGES  ON)

set(CMAKE_COLOR_MAKEFILE ON)

MESSAGE("AHEAD root CMAKE script. Including lib (Core) and tools (Queries):")

SET(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR})
SET(LIBRARY_OUTPUT_PATH ${CMAKE_BINARY_DIR})
SET(PROJECT_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/include)
SET(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/CMake")

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -Wall -Wextra -pedantic -Weffc++ -fexceptions -fnon-call-exceptions")
SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g3 -rdynamic")

INCLUDE(cotire)

INCLUDE_DIRECTORIES("${PROJECT_INCLUDE_DIR}")

UNSET(RESTINY32)
ADD_SUBDIRECTORY(lib)
ADD_SUBDIRECTORY(tools)

SET(RESTINY32 ON)
ADD_SUBDIRECTORY(tools tools_restiny32)
