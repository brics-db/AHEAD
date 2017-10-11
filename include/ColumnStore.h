// Copyright (c) 2016-2017 Till Kolditz
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
 * Created on 27-07-2016 18:14
 */

#ifndef COLUMNSTORE_H
#define COLUMNSTORE_H

// global type definitions

#include <util/v2types.hpp>
#include <util/resilience.hpp>

namespace ahead {

#define BITS_PER_BYTE 8
#define BITS_SIZEOF(x) (sizeof(x) * BITS_PER_BYTE)
#define BITS_TO_BYTES(x) (((x) + (BITS_PER_BYTE - 1)) / BITS_PER_BYTE)
    // the following macro takes into account the actual data width of "x", since we call clzll which converts it always into an unsigned long long
#define BITS_CLZ(x) (__builtin_clzll(x) - ((sizeof(unsigned long long) * BITS_PER_BYTE) - (sizeof(x) * BITS_PER_BYTE)))

    /*
     * Concatenate preprocessor tokens A and B without expanding macro definitions
     * (however, if invoked from a macro, macro arguments are expanded).
     */
#define CONCAT0(A, B) A B
#define CONCATX(A, B) CONCAT0(A, B)

    /*
     * Concatenate preprocessor tokens A and B after macro-expanding them.
     */
#define CONCAT(...) VFUNC(CONCAT, __VA_ARGS__)
#define CONCAT2(A, B) CONCATX(A, B)
#define CONCAT3(A, B, C) CONCAT2(CONCAT2(A, B), C)
#define CONCAT4(A, B, C, D) CONCAT2(CONCAT2(A, B), CONCAT2(C, D))
#define CONCAT5(A, B, C, D, E) CONCAT2(CONCAT4(A, B, C, D), E)

#define TOSTRING0(str) #str
#define TOSTRING(...) VFUNC(TOSTRING, __VA_ARGS__)
#define TOSTRING1(str) TOSTRING0(str)
#define TOSTRING2(str1, str2) TOSTRING0(str1) TOSTRING0(str2)
#define TOSTRING3(str1, str2, str3) TOSTRING0(CONCAT(str1, str2, str3))
#define TOSTRING4(str1, str2, str3, str4) TOSTRING0(CONCAT(str1, str2, str3, str4))

    // Fast-Forward Declarations
    // column_storage
    class AHEAD;
    class AHEAD_Config;
    class BucketManager;
    class ColumnManager;
    class TransactionManager;
    class BaseBAT;
    template<typename Head, typename Tail>
    class BAT;
    template<typename Tail>
    class ColumnBAT;
    template<class Head, class Tail>
    class TempBAT;

    // meta_repository
    class MetaRepositoryManager;

    typedef enum {
        left,
        right
    } hash_side_t;

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

    template<size_t alignment, typename T>
    T*
    align_to(
            T * const pT) {
        size_t tmp = reinterpret_cast<size_t>(pT);
        return reinterpret_cast<T*>(reinterpret_cast<uint8_t*>(pT) + (alignment - (tmp & (alignment - 1))));
    }
}

#endif /* COLUMNSTORE_H */

