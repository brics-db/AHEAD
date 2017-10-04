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

IF("${CMAKE_CURRENT_SOURCE_DIR}" STREQUAL "${CMAKE_SOURCE_DIR}")
    MESSAGE(FATAL_ERROR "You must run cmake on the root diretory!")
ENDIF()

IF(POLICY CMP0048)
    CMAKE_POLICY(SET CMP0048 NEW)
    PROJECT(LIBAHEAD VERSION 0.9 LANGUAGES CXX)
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
    SET_PROPERTY(TARGET ${LIBNAME} PROPERTY CXX_STANDARD 17)
    SET_PROPERTY(TARGET ${LIBNAME} PROPERTY CXX_STANDARD_REQUIRED ON)
    TARGET_LINK_LIBRARIES(${LIBNAME} ffi dl pthread curses boost_system boost_filesystem "${arg_dep_libs}")
    cotire(${LIBNAME})
ENDMACRO( add_static_lib )

MACRO( add_shared_lib arg_name arg_source_files arg_dep_libs )
    SET(LIBNAME "${arg_name}")

    STRING(REPLACE " " ";" SOURCE_FILES "${arg_source_files}")

    MESSAGE(STATUS "Add shared library ${LIBNAME}")
    ADD_LIBRARY(${LIBNAME} SHARED ${SOURCE_FILES})
    SET_PROPERTY(TARGET ${LIBNAME} PROPERTY CXX_STANDARD 17)
    SET_PROPERTY(TARGET ${LIBNAME} PROPERTY CXX_STANDARD_REQUIRED ON)
    TARGET_LINK_LIBRARIES(${LIBNAME} ffi dl pthread curses boost_system boost_filesystem "${arg_dep_libs}")
    cotire(${LIBNAME})
ENDMACRO( add_shared_lib )

MACRO(append_target_property arg_target arg_property arg_setting)
	GET_TARGET_PROPERTY(TEMP ${arg_target} ${arg_property})
	IF(TEMP STREQUAL "TEMP-NOTFOUND")
		SET(TEMP "") # set to empty string
	ELSE()
		SET(TEMP "${TEMP} ") # a space to cleanly separate from existing content
	ENDIF()
	# append our values
	SET(TEMP "${TEMP}${arg_setting}")
	SET_TARGET_PROPERTIES(${arg_target} PROPERTIES ${arg_property} ${TEMP})
ENDMACRO(append_target_property)

MESSAGE("Generating make scripts for ${PROJECT_NAME} in ${CMAKE_BUILD_TYPE} mode")

#add_static_lib(AHEAD "util/resilience.cpp \
add_shared_lib(AHEAD "AHEAD.cpp \
AHEAD_Config.cpp \
util/resilience.cpp \
util/rss.cpp \
util/stopwatch.cpp \
util/v2types.cpp \
util/argumentparser.cpp \
util/cpuid.cpp \
column_storage/ColumnManager.cpp \
column_storage/BucketManager.cpp \
column_storage/ColumnManager.cpp \
column_storage/Storage.cpp \
column_storage/Transaction.cpp \
column_storage/TransactionManager.cpp \
column_operators/Normal/aggregate.cpp \
column_operators/Normal/arithmetic.cpp \
column_operators/Normal/groupby.cpp \
column_operators/Normal/hashjoin_void.cpp \
column_operators/Normal/hashjoin_oid.cpp \
column_operators/Normal/hashjoin_id.cpp \
column_operators/Normal/hashjoin_size.cpp \
column_operators/Normal/hashjoin_tinyint.cpp \
column_operators/Normal/hashjoin_shortint.cpp \
column_operators/Normal/hashjoin_int.cpp \
column_operators/Normal/hashjoin_bigint.cpp \
column_operators/Normal/hashjoin_str.cpp \
column_operators/Normal/hashjoin_restiny.cpp \
column_operators/Normal/hashjoin_resshort.cpp \
column_operators/Normal/hashjoin_resint.cpp \
column_operators/Normal/hashjoin_resbigint.cpp \
column_operators/Normal/matchjoin_oid.cpp \
column_operators/Normal/matchjoin_id.cpp \
column_operators/Normal/matchjoin_size.cpp \
column_operators/Normal/matchjoin_tinyint.cpp \
column_operators/Normal/matchjoin_shortint.cpp \
column_operators/Normal/matchjoin_int.cpp \
column_operators/Normal/matchjoin_bigint.cpp \
column_operators/Normal/matchjoin_str.cpp \
column_operators/Normal/matchjoin_restiny.cpp \
column_operators/Normal/matchjoin_resshort.cpp \
column_operators/Normal/matchjoin_resint.cpp \
column_operators/Normal/matchjoin_resbigint.cpp \
column_operators/Normal/miscellaneous.cpp \
column_operators/Normal/select.cpp \
column_operators/AN/aggregateAN.cpp \
column_operators/AN/arithmeticAN.cpp \
column_operators/AN/encdecAN.cpp \
column_operators/AN/groupbyAN.cpp \
column_operators/AN/hashjoinAN_void.cpp \
column_operators/AN/hashjoinAN_oid.cpp \
column_operators/AN/hashjoinAN_id.cpp \
column_operators/AN/hashjoinAN_size.cpp \
column_operators/AN/hashjoinAN_tinyint.cpp \
column_operators/AN/hashjoinAN_shortint.cpp \
column_operators/AN/hashjoinAN_int.cpp \
column_operators/AN/hashjoinAN_bigint.cpp \
column_operators/AN/hashjoinAN_str.cpp \
column_operators/AN/hashjoinAN_resoid.cpp \
column_operators/AN/hashjoinAN_restiny.cpp \
column_operators/AN/hashjoinAN_resshort.cpp \
column_operators/AN/hashjoinAN_resint.cpp \
column_operators/AN/hashjoinAN_resbigint.cpp \
column_operators/AN/matchjoinAN.cpp \
column_operators/AN/selectAN_restiny.cpp \
column_operators/AN/selectAN_resshort.cpp \
column_operators/AN/selectAN_resint.cpp \
column_operators/AN/selectAN_str.cpp \
column_operators/SIMD/SSE.cpp \
column_operators/SIMD/AVX2.cpp \
meta_repository/MetaRepositoryManager.cpp" "")
TARGET_COMPILE_OPTIONS(AHEAD PUBLIC "-Wno-ignored-attributes")
