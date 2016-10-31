# author: Till Kolditz (C) 2016 Till.Kolditz@TU-Dresden.de

CMAKE_MINIMUM_REQUIRED(VERSION 3.5)
IF(POLICY CMP0048)
    CMAKE_POLICY(SET CMP0048 NEW)
    PROJECT(V2LIB VERSION 0.5 LANGUAGES CXX)
ELSE()
    PROJECT(V2LIB LANGUAGES CXX)
ENDIF()

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -Wall ${MACHINE_TYPE_FLAG} -march=native -fexceptions -fnon-call-exceptions")
#SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -DDEBUG -rdynamic -g")
#SET(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DNDEBUG")
SET(CMAKE_CXX_FLAGS_PROFILEGENERATE "${CMAKE_CXX_FLAGS_PROFILEGENERATE} -O0 -DDEBUG -rdynamic -g -pg -fprofile-generate")
SET(CMAKE_CXX_FLAGS_PROFILEUSE "${CMAKE_CXX_FLAGS_PROFILEUSE} -O3 -DNDEBUG -fprofile-use")

MACRO( add_static_lib arg_name arg_source_files arg_dep_libs )
    SET(LIBNAME "v2${arg_name}")

    STRING(REPLACE " " ";" TMP_SOURCE_FILES "${arg_source_files}")
    FOREACH(f ${TMP_SOURCE_FILES})
        LIST(APPEND SOURCE_FILES "${arg_name}/${f}")
    ENDFOREACH(f TMP_SOURCE_FILES)

    MESSAGE(STATUS "Add static library ${LIBNAME}")
    ADD_LIBRARY(${LIBNAME} STATIC ${SOURCE_FILES})
    TARGET_LINK_LIBRARIES(${LIBNAME} ffi dl pthread curses boost_system boost_filesystem)
ENDMACRO( add_static_lib )

MESSAGE("Generating make scripts for ${PROJECT_NAME} in ${CMAKE_BUILD_TYPE} mode")

add_static_lib(util "resilience.cpp rss.cpp stopwatch.cpp" "")
add_static_lib(column_storage "BucketManager.cpp ColumnManager.cpp Transaction.cpp TransactionManager.cpp" "v2util")
add_static_lib(meta_repository "MetaRepositoryManager.cpp" "v2util v2column_storage")
