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
		COMMAND git submodule add "https://github.com/opcm/pcm.git"
		WORKING_DIRECTORY "${ROOTDIR}")
ENDIF(NOT EXISTS "${PCMDIR}" OR NOT EXISTS "${LIBPCMDIR}")

# The same as above ---> build libpcm.so only once. Until we have a cmake project file in pcm/ the responsibility for recreating the library is on the user side
IF(NOT EXISTS "${LIBPCMDIR}/libpcm.so")
	MESSAGE(STATUS "Generating PCM library")
	EXECUTE_PROCESS(
		COMMAND make
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
    MESSAGE(STATUS "Add shared library ${LIBNAME}")
    SET(LIBNAME "${target_name}")
    STRING(REPLACE " " ";" SOURCE_FILES "${arg_source_files}")
    ADD_LIBRARY(${LIBNAME} SHARED ${SOURCE_FILES})
    SET_PROPERTY(TARGET ${LIBNAME} PROPERTY CXX_STANDARD 17)
    SET_PROPERTY(TARGET ${LIBNAME} PROPERTY CXX_STANDARD_REQUIRED ON)
    TARGET_INCLUDE_DIRECTORIES(${target_name} PUBLIC "${PCMDIR}")
    TARGET_LINK_LIBRARIES(${LIBNAME} AHEAD "${LIBPCM}" ffi dl pthread curses boost_system boost_filesystem)
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
    TARGET_LINK_LIBRARIES(${target_name} ${LIBPCM} AHEAD SSB)
ENDMACRO( add_algorithm )

MESSAGE("Generating make scripts for ${PROJECT_NAME} in ${CMAKE_BUILD_TYPE} mode")
MESSAGE(STATUS "link directory: ${LIBRARY_OUTPUT_PATH}")

#add_algorithm(main               "sample")
#add_algorithm(lineorder_size     "sample")
#add_algorithm(test_hashjoin      "sample")
#add_algorithm(test_group         "sample")

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

MACRO(add_ssb arg_queries)
	SET(QUERIES "")
    STRING(REPLACE " " ";" QUERIES "${arg_queries}")
    FOREACH(Q ${QUERIES})
        add_algorithm(${Q} "ssbm" "${Q}_scalar")
        TARGET_COMPILE_OPTIONS("${Q}_scalar" PUBLIC "-fno-tree-vectorize")
    ENDFOREACH(Q QUERIES)
ENDMACRO(add_ssb)

MACRO(add_ssb_SSE arg_queries)
	SET(QUERIES "")
    STRING(REPLACE " " ";" QUERIES "${arg_queries}")
    FOREACH(Q ${QUERIES})
        add_algorithm(${Q} "ssbm" "${Q}_SSE")
        TARGET_COMPILE_DEFINITIONS("${Q}_SSE" PUBLIC "FORCE_SSE")
    ENDFOREACH(Q QUERIES)
ENDMACRO(add_ssb_SSE)

add_ssb("ssbm-dbsize \
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

##############
### OPENMP ###
##############
MACRO(enable_openmp arg_queries)
	SET(QUERIES "")
    STRING(REPLACE " " ";" QUERIES "${arg_queries}")
    FOREACH(Q ${QUERIES})
        append_target_property(${Q} "COMPILE_FLAGS" "${OpenMP_CXX_FLAGS}")
        append_target_property(${Q} "LINK_FLAGS" "${OpenMP_CXX_FLAGS}")
    ENDFOREACH(Q QUERIES)
ENDMACRO(enable_openmp)

FIND_PACKAGE(OpenMP)

IF(OPENMP_FOUND)
	MESSAGE("Generating OpenMP targets")
	add_ssb("ssbm-q11_dmr_mt ssbm-q12_dmr_mt ssbm-q13_dmr_mt \
ssbm-q21_dmr_mt ssbm-q22_dmr_mt ssbm-q23_dmr_mt \
ssbm-q31_dmr_mt ssbm-q32_dmr_mt ssbm-q33_dmr_mt ssbm-q34_dmr_mt \
ssbm-q41_dmr_mt ssbm-q42_dmr_mt ssbm-q43_dmr_mt")
	add_ssb_SSE("ssbm-q11_dmr_mt ssbm-q12_dmr_mt ssbm-q13_dmr_mt \
ssbm-q21_dmr_mt ssbm-q22_dmr_mt ssbm-q23_dmr_mt \
ssbm-q31_dmr_mt ssbm-q32_dmr_mt ssbm-q33_dmr_mt ssbm-q34_dmr_mt \
ssbm-q41_dmr_mt ssbm-q42_dmr_mt ssbm-q43_dmr_mt")
	enable_openmp("ssbm-q11_dmr_mt_scalar ssbm-q12_dmr_mt_scalar ssbm-q13_dmr_mt_scalar ssbm-q11_dmr_mt_SSE ssbm-q12_dmr_mt_SSE ssbm-q13_dmr_mt_SSE \
ssbm-q21_dmr_mt_scalar ssbm-q22_dmr_mt_scalar ssbm-q23_dmr_mt_scalar ssbm-q21_dmr_mt_SSE ssbm-q22_dmr_mt_SSE ssbm-q23_dmr_mt_SSE \
ssbm-q31_dmr_mt_scalar ssbm-q32_dmr_mt_scalar ssbm-q33_dmr_mt_scalar ssbm-q34_dmr_mt_scalar ssbm-q31_dmr_mt_SSE ssbm-q32_dmr_mt_SSE ssbm-q33_dmr_mt_SSE ssbm-q34_dmr_mt_SSE \
ssbm-q41_dmr_mt_scalar ssbm-q42_dmr_mt_scalar ssbm-q43_dmr_mt_scalar ssbm-q41_dmr_mt_SSE ssbm-q42_dmr_mt_SSE ssbm-q43_dmr_mt_SSE")
ELSE(OPENMP_FOUND)
	MESSAGE(WARNING "OpenMP not found!")
ENDIF(OPENMP_FOUND)
