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
    PROJECT(SSBQueries VERSION 0.9 LANGUAGES CXX)
ELSE()
    PROJECT(SSBQueries LANGUAGES CXX)
ENDIF()

IF(DEFINED RESTINY32)
	SET(EXEC_SUFFIX "_restiny32")
ELSE()
	SET(EXEC_SUFFIX "")
ENDIF()

###########
### PCM ###
###########

# We check the following at configure time because we need to do this only once
SET(ROOTDIR "${CMAKE_CURRENT_SOURCE_DIR}/..")
SET(PCMDIR "${ROOTDIR}/pcm")
SET(LIBPCMDIR "${PCMDIR}/pcm.so")
MESSAGE("Checking for PCM")
IF(NOT EXISTS "${PCMDIR}" OR NOT EXISTS "${LIBPCMDIR}")
	MESSAGE(STATUS "Fetching PCM from github as submodule")
	EXECUTE_PROCESS(
		COMMAND git submodule add "https://github.com/brics-db/pcm.git" pcm
		WORKING_DIRECTORY "${ROOTDIR}")
	EXECUTE_PROCESS(
		COMMAND git submodule update --init pcm
		WORKING_DIRECTORY "${ROOTDIR}")
ENDIF(NOT EXISTS "${PCMDIR}" OR NOT EXISTS "${LIBPCMDIR}")

# The same as above ---> build libpcm.so only once. Until we have a cmake project file in pcm/ the responsibility for recreating the library is on the user side
IF(NOT EXISTS "${LIBPCMDIR}/libpcm.so")
	MESSAGE(STATUS "Generating PCM library")
	EXECUTE_PROCESS(
		COMMAND nproc
		OUTPUT_VARIABLE NPROC)
	EXECUTE_PROCESS(
		COMMAND make -j8
		WORKING_DIRECTORY "${LIBPCMDIR}")
ENDIF(NOT EXISTS "${LIBPCMDIR}/libpcm.so")

FIND_LIBRARY(LIBPCM pcm "${LIBPCMDIR}")
IF(LIBPCM)
	MESSAGE(STATUS "PCM library can be found (${LIBPCM})")
ENDIF(LIBPCM)

##########################
### SSB Shared Library ###
##########################
MACRO( add_shared_lib target_name arg_source_files )
    SET(LIBNAME "${target_name}")
    STRING(APPEND LIBNAME "${EXEC_SUFFIX}")
    MESSAGE(STATUS "Add shared library ${LIBNAME}")
    STRING(REPLACE " " ";" SOURCE_FILES "${arg_source_files}")
    ADD_LIBRARY(${LIBNAME} SHARED ${SOURCE_FILES})
    SET_PROPERTY(TARGET ${LIBNAME} PROPERTY CXX_STANDARD 17)
    SET_PROPERTY(TARGET ${LIBNAME} PROPERTY CXX_STANDARD_REQUIRED ON)
    TARGET_INCLUDE_DIRECTORIES(${LIBNAME} PUBLIC "${PCMDIR}")
    IF(LIBPCM)
        TARGET_LINK_LIBRARIES(${LIBNAME} AHEAD "${LIBPCM}" ffi dl pthread curses boost_system boost_filesystem)
        TARGET_COMPILE_DEFINITIONS(${LIBNAME} PUBLIC "AHEAD_PCM")
    ELSE(LIBPCM)
        TARGET_LINK_LIBRARIES(${LIBNAME} AHEAD ffi dl pthread curses boost_system boost_filesystem)
    ENDIF(LIBPCM)
ENDMACRO( add_shared_lib )
add_shared_lib(SSB "ssbm/ssb.cpp")

######################
### SAMPLES & SSBM ###
######################

MACRO( add_algorithm arg_name arg_path target_name )
    MESSAGE(STATUS "Add executable ${target_name}")
    FILE(GLOB SOURCE_FILES
        "${arg_path}/${arg_name}.cpp")
    ADD_EXECUTABLE(${target_name} ${SOURCE_FILES})
    SET_PROPERTY(TARGET ${target_name} PROPERTY CXX_STANDARD 17)
    SET_PROPERTY(TARGET ${target_name} PROPERTY CXX_STANDARD_REQUIRED ON)
	TARGET_INCLUDE_DIRECTORIES(${target_name} PUBLIC "${PCMDIR}")
    IF(LIBPCM)
        TARGET_LINK_LIBRARIES(${target_name} ${LIBPCM} AHEAD SSB)
        TARGET_COMPILE_DEFINITIONS(${LIBNAME} PUBLIC "AHEAD_PCM")
    ELSE(LIBPCM)
        TARGET_LINK_LIBRARIES(${target_name} AHEAD SSB)
    ENDIF(LIBPCM)
    IF(DEFINED RESTINY32)
    	TARGET_COMPILE_DEFINITIONS(${target_name} PUBLIC "RESTINY32")
    ENDIF(DEFINED RESTINY32)
ENDMACRO( add_algorithm )

MESSAGE("Generating make scripts for ${PROJECT_NAME} in ${CMAKE_BUILD_TYPE} mode")
MESSAGE(STATUS "link directory: ${LIBRARY_OUTPUT_PATH}")

MACRO(add_ssb_Scalar arg_queries)
	SET(QUERIES "")
    STRING(REPLACE " " ";" QUERIES "${arg_queries}")
    FOREACH(Q ${QUERIES})
    	SET(EXEC_NAME "${Q}_scalar")
    	STRING(APPEND EXEC_NAME "${EXEC_SUFFIX}")
        add_algorithm(${Q} "ssbm" "${EXEC_NAME}")
        TARGET_COMPILE_OPTIONS("${EXEC_NAME}" PUBLIC "-fno-tree-vectorize")
    ENDFOREACH(Q QUERIES)
ENDMACRO(add_ssb_Scalar)

MACRO(add_ssb_SSE arg_queries)
	SET(QUERIES "")
    STRING(REPLACE " " ";" QUERIES "${arg_queries}")
    FOREACH(Q ${QUERIES})
    	SET(EXEC_NAME "${Q}_SSE")
    	STRING(APPEND EXEC_NAME "${EXEC_SUFFIX}")
        add_algorithm(${Q} "ssbm" "${EXEC_NAME}")
        TARGET_COMPILE_DEFINITIONS("${EXEC_NAME}" PUBLIC "FORCE_SSE")
    ENDFOREACH(Q QUERIES)
ENDMACRO(add_ssb_SSE)

# If you want to be able to test the restiny_t=32bit behavior for *all* queries, simply remove the if-branch, so that all queries will be generated a second time with the larger data type!
IF(DEFINED RESTINY32)
	add_ssb_Scalar("ssbm-dbsize ssbm-q11_normal ssbm-q11_dmr_seq ssbm-q11_early ssbm-q11_late ssbm-q11_continuous ssbm-q11_continuous_reenc")
	add_ssb_SSE("ssbm-q11_normal ssbm-q11_dmr_seq ssbm-q11_early ssbm-q11_late ssbm-q11_continuous ssbm-q11_continuous_reenc")
ELSE()
	add_ssb_Scalar("ssbm-dbsize \
ssbm-q11_normal ssbm-q11_dmr_seq ssbm-q11_early ssbm-q11_late ssbm-q11_continuous ssbm-q11_continuous_reenc \
ssbm-q12_normal ssbm-q12_dmr_seq ssbm-q12_early ssbm-q12_late ssbm-q12_continuous ssbm-q12_continuous_reenc \
ssbm-q13_normal ssbm-q13_dmr_seq ssbm-q13_early ssbm-q13_late ssbm-q13_continuous ssbm-q13_continuous_reenc \
ssbm-q21_normal ssbm-q21_dmr_seq ssbm-q21_early ssbm-q21_late ssbm-q21_continuous ssbm-q21_continuous_reenc \
ssbm-q22_normal ssbm-q22_dmr_seq ssbm-q22_early ssbm-q22_late ssbm-q22_continuous ssbm-q22_continuous_reenc \
ssbm-q23_normal ssbm-q23_dmr_seq ssbm-q23_early ssbm-q23_late ssbm-q23_continuous ssbm-q23_continuous_reenc \
ssbm-q31_normal ssbm-q31_dmr_seq ssbm-q31_early ssbm-q31_late ssbm-q31_continuous ssbm-q31_continuous_reenc \
ssbm-q32_normal ssbm-q32_dmr_seq ssbm-q32_early ssbm-q32_late ssbm-q32_continuous ssbm-q32_continuous_reenc \
ssbm-q33_normal ssbm-q33_dmr_seq ssbm-q33_early ssbm-q33_late ssbm-q33_continuous ssbm-q33_continuous_reenc \
ssbm-q34_normal ssbm-q34_dmr_seq ssbm-q34_early ssbm-q34_late ssbm-q34_continuous ssbm-q34_continuous_reenc \
ssbm-q41_normal ssbm-q41_dmr_seq ssbm-q41_early ssbm-q41_late ssbm-q41_continuous ssbm-q41_continuous_reenc \
ssbm-q42_normal ssbm-q42_dmr_seq ssbm-q42_early ssbm-q42_late ssbm-q42_continuous ssbm-q42_continuous_reenc \
ssbm-q43_normal ssbm-q43_dmr_seq ssbm-q43_early ssbm-q43_late ssbm-q43_continuous ssbm-q43_continuous_reenc")
	add_ssb_SSE("ssbm-q11_normal ssbm-q11_dmr_seq ssbm-q11_early ssbm-q11_late ssbm-q11_continuous ssbm-q11_continuous_reenc \
ssbm-q12_normal ssbm-q12_dmr_seq ssbm-q12_early ssbm-q12_late ssbm-q12_continuous ssbm-q12_continuous_reenc \
ssbm-q13_normal ssbm-q13_dmr_seq ssbm-q13_early ssbm-q13_late ssbm-q13_continuous ssbm-q13_continuous_reenc \
ssbm-q21_normal ssbm-q21_dmr_seq ssbm-q21_early ssbm-q21_late ssbm-q21_continuous ssbm-q21_continuous_reenc \
ssbm-q22_normal ssbm-q22_dmr_seq ssbm-q22_early ssbm-q22_late ssbm-q22_continuous ssbm-q22_continuous_reenc \
ssbm-q23_normal ssbm-q23_dmr_seq ssbm-q23_early ssbm-q23_late ssbm-q23_continuous ssbm-q23_continuous_reenc \
ssbm-q31_normal ssbm-q31_dmr_seq ssbm-q31_early ssbm-q31_late ssbm-q31_continuous ssbm-q31_continuous_reenc \
ssbm-q32_normal ssbm-q32_dmr_seq ssbm-q32_early ssbm-q32_late ssbm-q32_continuous ssbm-q32_continuous_reenc \
ssbm-q33_normal ssbm-q33_dmr_seq ssbm-q33_early ssbm-q33_late ssbm-q33_continuous ssbm-q33_continuous_reenc \
ssbm-q34_normal ssbm-q34_dmr_seq ssbm-q34_early ssbm-q34_late ssbm-q34_continuous ssbm-q34_continuous_reenc \
ssbm-q41_normal ssbm-q41_dmr_seq ssbm-q41_early ssbm-q41_late ssbm-q41_continuous ssbm-q41_continuous_reenc \
ssbm-q42_normal ssbm-q42_dmr_seq ssbm-q42_early ssbm-q42_late ssbm-q42_continuous ssbm-q42_continuous_reenc \
ssbm-q43_normal ssbm-q43_dmr_seq ssbm-q43_early ssbm-q43_late ssbm-q43_continuous ssbm-q43_continuous_reenc")
ENDIF()
