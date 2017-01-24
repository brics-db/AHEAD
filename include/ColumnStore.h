// Copyright (c) 2016 Till Kolditz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/* 
 * File:   ColumnStore.h
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 27. Juli 2016, 18:14
 */

#ifndef COLUMNSTORE_H
#define COLUMNSTORE_H

// global type definitions

#include <util/v2types.hpp>
#include <util/resilience.hpp>

#define TOSTRING0(str) #str
#define TOSTRING(str) TOSTRING0(str)

/*
 * Concatenate preprocessor tokens A and B without expanding macro definitions
 * (however, if invoked from a macro, macro arguments are expanded).
 */
#define CONCAT0(A, B) A ## B

/*
 * Concatenate preprocessor tokens A and B after macro-expanding them.
 */
#define CONCAT(A, B) CONCAT0(A, B)

/***********************************************************/
/* Definition of meta-macro for variable-length macros     */
/***********************************************************/
// get number of arguments with __NARG__
#define __NARG__(...)  __NARG_I_(__VA_ARGS__,__RSEQ_N())
#define __NARG_I_(...) __ARG_N(__VA_ARGS__)
#define __ARG_N( \
      _1, _2, _3, _4, _5, _6, _7, _8, _9,_10, \
     _11,_12,_13,_14,_15,_16,_17,_18,_19,_20, \
     _21,_22,_23,_24,_25,_26,_27,_28,_29,_30, \
     _31,_32,_33,_34,_35,_36,_37,_38,_39,_40, \
     _41,_42,_43,_44,_45,_46,_47,_48,_49,_50, \
     _51,_52,_53,_54,_55,_56,_57,_58,_59,_60, \
     _61,_62,_63,N,...) N
#define __RSEQ_N() \
     63,62,61,60,                   \
     59,58,57,56,55,54,53,52,51,50, \
     49,48,47,46,45,44,43,42,41,40, \
     39,38,37,36,35,34,33,32,31,30, \
     29,28,27,26,25,24,23,22,21,20, \
     19,18,17,16,15,14,13,12,11,10, \
     9,8,7,6,5,4,3,2,1,0

// general definition for any function name
#define _VFUNC_(name, n) name##n
#define _VFUNC(name, n) _VFUNC_(name, n)
#define VFUNC(func, ...) _VFUNC(func, __NARG__(__VA_ARGS__)) (__VA_ARGS__)

// Example definition for FOO
// #define FOO(...) VFUNC(FOO, __VA_ARGS__)
// #define FOO2(x, y) ((x) + (y))
// #define FOO3(x, y, z) ((x) + (y) + (z))
// it also works with C functions:
// int FOO4(int a, int b, int c, int d) { return a + b + c + d; }

/***********************************************************/

// Fast-Forward Declarations

// column_storage
class BucketManager;
class ColumnManager;
class TransactionManager;
template<typename Head, typename Tail>
class BAT;
template<typename Tail>
class ColumnBAT;
template<class Head, class Tail>
class TempBAT;

// meta_repository
class MetaRepositoryManager;

// Fast-Forward declare Bat, ColumnBAT, TempBat types
typedef ColumnBAT<v2_tinyint_t> tinyint_colbat_t;
typedef ColumnBAT<v2_shortint_t> shortint_colbat_t;
typedef ColumnBAT<v2_int_t> int_colbat_t;
typedef ColumnBAT<v2_bigint_t> bigint_colbat_t;
typedef ColumnBAT<v2_char_t> char_colbat_t;
typedef ColumnBAT<v2_str_t> str_colbat_t;
typedef ColumnBAT<v2_fixed_t> fixed_colbat_t;
typedef ColumnBAT<v2_oid_t> oid_colbat_t;
typedef ColumnBAT<v2_id_t> id_colbat_t;
typedef ColumnBAT<v2_version_t> version_colbat_t;
typedef ColumnBAT<v2_size_t> size_colbat_t;

typedef TempBAT<v2_void_t, v2_tinyint_t> tinyint_tmpbat_t;
typedef TempBAT<v2_void_t, v2_shortint_t> shortint_tmpbat_t;
typedef TempBAT<v2_void_t, v2_int_t> int_tmpbat_t;
typedef TempBAT<v2_void_t, v2_bigint_t> bigint_tmpbat_t;
typedef TempBAT<v2_void_t, v2_char_t> char_tmpbat_t;
typedef TempBAT<v2_void_t, v2_str_t> str_tmpbat_t;
typedef TempBAT<v2_void_t, v2_fixed_t> fixed_tmpbat_t;
typedef TempBAT<v2_void_t, v2_oid_t> oid_tmpbat_t;
typedef TempBAT<v2_void_t, v2_id_t> id_tmpbat_t;
typedef TempBAT<v2_void_t, v2_version_t> version_tmpbat_t;
typedef TempBAT<v2_void_t, v2_size_t> size_tmpbat_t;

// Fast-Forward declare ColumnBAT, TempBat types
typedef ColumnBAT<v2_restiny_t> restiny_colbat_t;
typedef ColumnBAT<v2_resshort_t> resshort_colbat_t;
typedef ColumnBAT<v2_resint_t> resint_colbat_t;
typedef ColumnBAT<v2_resoid_t> resoid_colbat_t;
typedef ColumnBAT<v2_resstr_t> resstr_colbat_t;

typedef TempBAT<v2_void_t, v2_restiny_t> restiny_tmpbat_t;
typedef TempBAT<v2_void_t, v2_resshort_t> resshort_tmpbat_t;
typedef TempBAT<v2_void_t, v2_resint_t> resint_tmpbat_t;
typedef TempBAT<v2_void_t, v2_resoid_t> resoid_tmpbat_t;
typedef TempBAT<v2_void_t, v2_resstr_t> resstr_tmpbat_t;

#endif /* COLUMNSTORE_H */

