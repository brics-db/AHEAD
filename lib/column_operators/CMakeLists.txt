# author: Till Kolditz
# created: 2016-07-27 14:00

cmake_minimum_required(VERSION 3.5)

set(CMAKE_C_COMPILER_INIT "CC=clang CXX=clang++")

project(column_operators)

find_package(LLVM REQUIRED CONFIG)
message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")
include_directories(${LLVM_INCLUDE_DIRS})
add_definitions(${LLVM_DEFINITIONS})

add_definitions(-march=native)
add_definitions(-std=c++14)
add_definitions(-O3)
add_definitions(-Wall)
add_definitions(-Wextra)

set(SOURCES operators.cpp)


#set (LIBS
#    ${LLVM_LIBRARIES_PATH}/LLVMSupport.lib
#    ${LLVM_LIBRARIES_PATH}/LLVMCore.lib
#    ${LLVM_LIBRARIES_PATH}/LLVMMC.lib
#    ${LLVM_LIBRARIES_PATH}/LLVMTarget.lib
#    ${LLVM_LIBRARIES_PATH}/LLVMAnalysis.lib
#    ${LLVM_LIBRARIES_PATH}/LLVMipa.lib
#    ${LLVM_LIBRARIES_PATH}/LLVMTransformUtils.lib
#    ${LLVM_LIBRARIES_PATH}/LLVMInstCombine.lib
#    ${LLVM_LIBRARIES_PATH}/LLVMScalarOpts.lib
#    ${LLVM_LIBRARIES_PATH}/LLVMCodeGen.lib
#    ${LLVM_LIBRARIES_PATH}/LLVMExecutionEngine.lib
#    ${LLVM_LIBRARIES_PATH}/LLVMJIT.lib
#    ${LLVM_LIBRARIES_PATH}/LLVMX86Utils.lib
#    ${LLVM_LIBRARIES_PATH}/LLVMX86Info.lib
#    ${LLVM_LIBRARIES_PATH}/LLVMMCParser.lib
#    ${LLVM_LIBRARIES_PATH}/LLVMX86AsmParser.lib
#    ${LLVM_LIBRARIES_PATH}/LLVMX86AsmPrinter.lib
#    ${LLVM_LIBRARIES_PATH}/LLVMAsmPrinter.lib
#    ${LLVM_LIBRARIES_PATH}/LLVMSelectionDAG.lib
#    ${LLVM_LIBRARIES_PATH}/LLVMX86CodeGen.lib
#    ${LLVM_LIBRARIES_PATH}/LLVMX86Disassembler.lib
#    ${LLVM_LIBRARIES_PATH}/LLVMInterpreter.lib    
#)

add_library(column_operators STATIC ${SOURCES})

