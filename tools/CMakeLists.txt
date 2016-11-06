# author: Till Kolditz (C) 2016 Till.Kolditz@TU-Dresden.de

CMAKE_MINIMUM_REQUIRED(VERSION 3.1 FATAL_ERROR)

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
    TARGET_LINK_LIBRARIES(${arg_name} v2util v2column_storage v2meta_repository)
    MESSAGE(STATUS "Add executable ${arg_name}")
ENDMACRO( add_algorithm )

MESSAGE("Generating make scripts for ${PROJECT_NAME} in ${CMAKE_BUILD_TYPE} mode")
MESSAGE(STATUS "link directory: ${LIBRARY_OUTPUT_PATH}")

add_algorithm(main               "sample")
add_algorithm(lineorder_size     "sample")
add_algorithm(test_hashjoin      "sample")
add_algorithm(test_group         "sample")

MACRO(add_ssbm arg_queries)
    STRING(REPLACE " " ";" QUERIES "${arg_queries}")
    FOREACH(Q ${QUERIES})
        add_algorithm(${Q} "ssbm")
    ENDFOREACH(Q QUERIES)
ENDMACRO(add_ssbm)

add_ssbm("ssbm-q11 ssbm-q11_early ssbm-q11_late ssbm-q11_continuous ssbm-q12 ssbm-q12_early ssbm-q12_late ssbm-q12_continuous ssbm-q13 ssbm-q13_early ssbm-q13_late ssbm-q13_continuous")
