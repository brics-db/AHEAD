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

CMAKE_MINIMUM_REQUIRED(VERSION 3.5 FATAL_ERROR)

SET(CMAKE_C_COMPILER_INIT gcc-6)
SET(CMAKE_CXX_COMPILER_INIT g++-6)

IF(POLICY CMP0048)
    CMAKE_POLICY(SET CMP0048 NEW)
    PROJECT(LIBAHEAD VERSION 0.7 LANGUAGES CXX)
ELSE()
    PROJECT(LIBAHEAD LANGUAGES CXX)
ENDIF()

MACRO( add_static_lib arg_name arg_source_files arg_dep_libs )
    SET(LIBNAME "${arg_name}")

    STRING(REPLACE " " ";" SOURCE_FILES "${arg_source_files}")
    #FOREACH(f ${TMP_SOURCE_FILES})
    #    LIST(APPEND SOURCE_FILES "${arg_name}/${f}")
    #ENDFOREACH(f TMP_SOURCE_FILES)

    MESSAGE(STATUS "Add static library ${LIBNAME}")
    ADD_LIBRARY(${LIBNAME} STATIC ${SOURCE_FILES})
    SET_PROPERTY(TARGET ${LIBNAME} PROPERTY CXX_STANDARD 14)
    SET_PROPERTY(TARGET ${LIBNAME} PROPERTY CXX_STANDARD_REQUIRED ON)
    TARGET_LINK_LIBRARIES(${LIBNAME} ffi dl pthread curses boost_system boost_filesystem "${arg_dep_libs}")
ENDMACRO( add_static_lib )

MACRO( add_shared_lib arg_name arg_source_files arg_dep_libs )
    SET(LIBNAME "${arg_name}")

    STRING(REPLACE " " ";" SOURCE_FILES "${arg_source_files}")

    MESSAGE(STATUS "Add shared library ${LIBNAME}")
    ADD_LIBRARY(${LIBNAME} SHARED ${SOURCE_FILES})
    SET_PROPERTY(TARGET ${LIBNAME} PROPERTY CXX_STANDARD 14)
    SET_PROPERTY(TARGET ${LIBNAME} PROPERTY CXX_STANDARD_REQUIRED ON)
    TARGET_LINK_LIBRARIES(${LIBNAME} ffi dl pthread curses boost_system boost_filesystem "${arg_dep_libs}")
ENDMACRO( add_shared_lib )


MESSAGE("Generating make scripts for ${PROJECT_NAME} in ${CMAKE_BUILD_TYPE} mode")

#add_static_lib(AHEAD "util/resilience.cpp \
add_shared_lib(AHEAD "AHEAD.cpp \
util/resilience.cpp \
util/rss.cpp \
util/stopwatch.cpp \
util/v2types.cpp \
util/argumentparser.cpp \
column_storage/ColumnManager.cpp \
column_storage/BucketManager.cpp \
column_storage/ColumnManager.cpp \
column_storage/Storage.cpp \
column_storage/Transaction.cpp \
column_storage/TransactionManager.cpp \
column_operators/Normal/aggregate.cpp \
column_operators/Normal/groupby.cpp \
column_operators/Normal/hashjoin.cpp \
column_operators/Normal/matchjoin.cpp \
column_operators/Normal/miscellaneous.cpp \
column_operators/Normal/select.cpp \
column_operators/AN/aggregateAN.cpp \
column_operators/AN/groupbyAN.cpp \
column_operators/AN/hashjoinAN.cpp \
column_operators/AN/matchjoinAN.cpp \
column_operators/AN/selectAN.cpp \
column_operators/SSE.cpp \
meta_repository/MetaRepositoryManager.cpp" "")
