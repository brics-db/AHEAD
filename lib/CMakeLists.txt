# author: Till Kolditz (C) 2016 Till.Kolditz@TU-Dresden.de

CMAKE_MINIMUM_REQUIRED(VERSION 3.1 FATAL_ERROR)

SET(CMAKE_C_COMPILER_INIT gcc-6)
SET(CMAKE_CXX_COMPILER_INIT g++-6)

IF(POLICY CMP0048)
    CMAKE_POLICY(SET CMP0048 NEW)
    PROJECT(V2LIB VERSION 0.5 LANGUAGES CXX)
ELSE()
    PROJECT(V2LIB LANGUAGES CXX)
ENDIF()

MACRO( add_static_lib arg_name arg_source_files arg_dep_libs )
    SET(LIBNAME "${arg_name}")

    STRING(REPLACE " " ";" SOURCE_FILES "${arg_source_files}")
    #FOREACH(f ${TMP_SOURCE_FILES})
    #    LIST(APPEND SOURCE_FILES "${arg_name}/${f}")
    #ENDFOREACH(f TMP_SOURCE_FILES)

    MESSAGE(STATUS "Add static library ${LIBNAME}")
    ADD_LIBRARY(${LIBNAME} SHARED ${SOURCE_FILES})
    SET_PROPERTY(TARGET ${LIBNAME} PROPERTY CXX_STANDARD 14)
    SET_PROPERTY(TARGET ${LIBNAME} PROPERTY CXX_STANDARD_REQUIRED ON)
    TARGET_LINK_LIBRARIES(${LIBNAME} ffi dl pthread curses boost_system boost_filesystem)
ENDMACRO( add_static_lib )

MESSAGE("Generating make scripts for ${PROJECT_NAME} in ${CMAKE_BUILD_TYPE} mode")

#add_static_lib(util "resilience.cpp rss.cpp stopwatch.cpp v2types.cpp argumentparser.cpp" "")
#add_static_lib(column_storage "BucketManager.cpp ColumnManager.cpp Transaction.cpp TransactionManager.cpp" "v2util")
#add_static_lib(column_operators "Operators.cpp" "v2util v2column_storage")
#add_static_lib(meta_repository "MetaRepositoryManager.cpp" "v2util v2column_storage, v2column_operators")
add_static_lib(v2core "util/resilience.cpp util/rss.cpp util/stopwatch.cpp util/v2types.cpp util/argumentparser.cpp column_storage/BucketManager.cpp column_storage/ColumnManager.cpp column_storage/Transaction.cpp column_storage/TransactionManager.cpp column_operators/Operators.cpp meta_repository/MetaRepositoryManager.cpp" "")
