# author: Till Kolditz (C) 2016 Till.Kolditz@TU-Dresden.de

CMAKE_MINIMUM_REQUIRED(VERSION 3.5)
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
    TARGET_LINK_LIBRARIES(${arg_name} v2util v2column_storage v2meta_repository)
    MESSAGE(STATUS "Add executable ${arg_name}")
ENDMACRO( add_algorithm )

MESSAGE("Generating make scripts for ${PROJECT_NAME} in ${CMAKE_BUILD_TYPE} mode")
MESSAGE(STATUS "link directory: ${LIBRARY_OUTPUT_PATH}")

add_algorithm(main               "sample")
add_algorithm(lineorder_size     "sample")
add_algorithm(test_hashjoin      "sample")

MACRO(add_ssbm arg_queries)
    STRING(REPLACE " " ";" QUERIES "${arg_queries}")
    FOREACH(Q ${QUERIES})
        add_algorithm(${Q} "ssbm")
    ENDFOREACH(Q QUERIES)
ENDMACRO(add_ssbm)

add_ssbm("ssbm-q11 ssbm-q11_eager ssbm-q11_lazy ssbm-q11_encoded ssbm-q12 ssbm-q12_eager ssbm-q12_lazy ssbm-q12_encoded ssbm-q13 ssbm-q13_eager ssbm-q13_lazy ssbm-q13_encoded")
