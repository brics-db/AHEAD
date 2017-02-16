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
    PROJECT(RSTORETOOLS VERSION 1.0 LANGUAGES CXX)
ELSE()
    PROJECT(RSTORETOOLS LANGUAGES CXX)
ENDIF()

###########
### PCM ###
###########

# We check the following at configure time because we need to do this only once
SET(ROOTDIR "${CMAKE_CURRENT_SOURCE_DIR}/..")
SET(PCMDIR "${ROOTDIR}/pcm")
SET(LIBPCMDIR "${PCMDIR}/pcm.so")
MESSAGE("Checking for PCM")
IF(NOT EXISTS "${PCMDIR}")
	MESSAGE(STATUS "Fetching PCM from github as submodule")
	EXECUTE_PROCESS(
		COMMAND git submodule add "https://github.com/opcm/pcm.git"
		WORKING_DIRECTORY "${ROOTDIR}")
ENDIF(NOT EXISTS "${PCMDIR}")

# The same as above ---> build libpcm.so only once. Until we have a cmake project file in pcm/ the responsibility for recreating the library is on the user side
IF(NOT EXISTS "${LIBPCMDIR}/libpcm.so")
	MESSAGE(STATUS "Generating PCM library")
	EXECUTE_PROCESS(
		COMMAND make
		WORKING_DIRECTORY "${LIBPCMDIR}")
ENDIF(NOT EXISTS "${LIBPCMDIR}/libpcm.so")

FIND_LIBRARY(LIBPCM pcm "${LIBPCMDIR}")
IF(LIBPCM)
	MESSAGE(STATUS "PCM library can be found")
ENDIF(LIBPCM)

#######################
### SAMPLES & TOOLS ###
#######################

MACRO( add_algorithm arg_name arg_path )
    FILE(GLOB SOURCE_FILES
        "${arg_path}/${arg_name}.cpp")
    ADD_EXECUTABLE(${arg_name} ${SOURCE_FILES})
    SET_PROPERTY(TARGET ${arg_name} PROPERTY CXX_STANDARD 14)
    SET_PROPERTY(TARGET ${arg_name} PROPERTY CXX_STANDARD_REQUIRED ON)
	TARGET_INCLUDE_DIRECTORIES(${arg_name} PUBLIC "${PCMDIR}")
    TARGET_LINK_LIBRARIES(${arg_name} ${LIBPCM} RStoreCore)
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
        APPEND_TARGET_PROPERTY(${Q} "COMPILE_FLAGS" "${OpenMP_CXX_FLAGS}")
        APPEND_TARGET_PROPERTY(${Q} "LINK_FLAGS" "${OpenMP_CXX_FLAGS}")
    ENDFOREACH(Q QUERIES)
ENDMACRO(enable_openmp)

add_ssbm("ssbm-q11_normal ssbm-q11_dmr_seq ssbm-q11_dmr_mt ssbm-q11_early ssbm-q11_late ssbm-q11_continuous ssbm-q11_continuous_reenc ssbm-q12_normal ssbm-q12_dmr_seq ssbm-q12_dmr_mt ssbm-q12_early ssbm-q12_late ssbm-q12_continuous ssbm-q12_continuous_reenc ssbm-q13_normal ssbm-q13_dmr_seq ssbm-q13_dmr_mt ssbm-q13_early ssbm-q13_late ssbm-q13_continuous ssbm-q13_continuous_reenc ssbm-q21_normal ssbm-q21_dmr_seq ssbm-q21_dmr_mt ssbm-q21_early ssbm-q21_late ssbm-q21_continuous ssbm-q21_continuous_reenc")

##############
### OPENMP ###
##############
FIND_PACKAGE(OpenMP)
IF(OPENMP_FOUND)
	enable_openmp("ssbm-q11_dmr_mt ssbm-q12_dmr_mt ssbm-q13_dmr_mt ssbm-q21_dmr_mt")
ENDIF(OPENMP_FOUND)
