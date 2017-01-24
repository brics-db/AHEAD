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

CMAKE_MINIMUM_REQUIRED(VERSION 3.1 FATAL_ERROR)

SET(CMAKE_C_COMPILER_INIT gcc-6)
SET(CMAKE_CXX_COMPILER_INIT g++-6)

IF(POLICY CMP0048)
    CMAKE_POLICY(SET CMP0048 NEW)
    PROJECT(V2TOOLS VERSION 0.5 LANGUAGES CXX)
ELSE()
    PROJECT(V2TOOLS LANGUAGES CXX)
ENDIF()

MACRO( add_algorithm arg_name arg_path )
    FILE(GLOB SOURCE_FILES
        "${arg_path}/${arg_name}.cpp")
    ADD_EXECUTABLE(${arg_name} ${SOURCE_FILES})
    SET_PROPERTY(TARGET ${arg_name} PROPERTY CXX_STANDARD 14)
    SET_PROPERTY(TARGET ${arg_name} PROPERTY CXX_STANDARD_REQUIRED ON)
    TARGET_LINK_LIBRARIES(${arg_name} v2core)
    MESSAGE(STATUS "Add executable ${arg_name}")
ENDMACRO( add_algorithm )

MESSAGE("Generating make scripts for ${PROJECT_NAME} in ${CMAKE_BUILD_TYPE} mode")
MESSAGE(STATUS "link directory: ${LIBRARY_OUTPUT_PATH}")

#add_algorithm(main               "sample")
#add_algorithm(lineorder_size     "sample")
#add_algorithm(test_hashjoin      "sample")
#add_algorithm(test_group         "sample")

MACRO(add_ssbm arg_queries)
    STRING(REPLACE " " ";" QUERIES "${arg_queries}")
    FOREACH(Q ${QUERIES})
        add_algorithm(${Q} "ssbm")
    ENDFOREACH(Q QUERIES)
ENDMACRO(add_ssbm)

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

MACRO(enable_openmp arg_queries)
    STRING(REPLACE " " ";" QUERIES "${arg_queries}")
    FOREACH(Q ${QUERIES})
        append_target_property(${Q} "COMPILE_FLAGS" "-fopenmp")
        append_target_property(${Q} "LINK_FLAGS" "-fopenmp")
    ENDFOREACH(Q QUERIES)
ENDMACRO(add_ssbm)

add_ssbm("ssbm-q11_normal ssbm-q11_dmr_seq ssbm-q11_dmr_mt ssbm-q11_early ssbm-q11_late ssbm-q11_continuous ssbm-q11_continuous_reenc ssbm-q12_normal ssbm-q12_dmr_seq ssbm-q12_dmr_mt ssbm-q12_early ssbm-q12_late ssbm-q12_continuous ssbm-q12_continuous_reenc ssbm-q13_normal ssbm-q13_dmr_seq ssbm-q13_dmr_mt ssbm-q13_early ssbm-q13_late ssbm-q13_continuous ssbm-q13_continuous_reenc ssbm-q21_normal ssbm-q21_dmr_seq ssbm-q21_dmr_mt ssbm-q21_early ssbm-q21_late ssbm-q21_continuous ssbm-q21_continuous_reenc")

enable_openmp("ssbm-q11_dmr_mt ssbm-q12_dmr_mt ssbm-q13_dmr_mt ssbm-q21_dmr_mt")
