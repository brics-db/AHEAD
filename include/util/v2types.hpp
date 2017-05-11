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
 * File:   v2types.hpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 19. November 2016, 23:52
 */

#ifndef V2TYPES_HPP
#define V2TYPES_HPP

#include <cinttypes>
#include <utility>
#include <functional>

namespace ahead {

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

    /* META MACRO intended for generating type names */
#define MKT(...) VFUNC(MKT, __VA_ARGS__)
#define MKT1(BASE) CONCAT(BASE, _t)
#define MKT2(BASE, SUFFIX) MKT1(CONCAT(BASE, SUFFIX))
#define MKT3(PREFIX, BASE, SUFFIX) CONCAT(PREFIX, CONCAT(BASE, SUFFIX))

    template<typename T, typename U, bool>
    struct type_selector {

        typedef T type_t;
    };

    template<typename T, typename U>
    struct type_selector<T, U, false> {

        typedef U type_t;
    };

    template<typename T, typename U>
    struct larger_type : public type_selector<T, U, (sizeof(T) >= sizeof(U))> {

        using type_t = typename type_selector<T, U, (sizeof (T) >= sizeof (U))>::type_t;

        constexpr static const bool areEquallyLarge = sizeof(T) == sizeof(U);
        constexpr static const bool isFirstLarger = sizeof(T) > sizeof(U);
        constexpr static const bool isSecondLarger = sizeof(T) < sizeof(U);
    };

    template<typename T, typename U>
    struct smaller_type : public type_selector<T, U, (sizeof(T) < sizeof(U))> {

        using type_t = typename type_selector<T, U, (sizeof (T) < sizeof (U))>::type_t;

        constexpr static const bool areEquallySmall = sizeof(T) == sizeof(U);
        constexpr static const bool isFirstSmaller = sizeof(T) < sizeof(U);
        constexpr static const bool isSecondSmaller = sizeof(T) > sizeof(U);
    };

    template<typename T, typename U, bool>
    struct v2_type_selector {

        typedef T type_t;
    };

    template<typename T, typename U>
    struct v2_type_selector<T, U, false> {

        typedef U type_t;
    };

    template<typename T, typename U>
    struct v2_larger_type : public type_selector<T, U, (sizeof(typename T::type_t) >= sizeof(typename U::type_t))> {
        typedef typename T::type_t ttype_t;
        typedef typename U::type_t utype_t;
        using type_t = typename type_selector<T, U, (sizeof(ttype_t) >= sizeof(utype_t))>::type_t;

        constexpr static const bool areEquallyLarge = sizeof(ttype_t) == sizeof(utype_t);
        constexpr static const bool isFirstLarger = sizeof(ttype_t) > sizeof(utype_t);
        constexpr static const bool isSecondLarger = sizeof(ttype_t) < sizeof(utype_t);
    };

    template<typename T, typename U>
    struct v2_smaller_type : public type_selector<T, U, (sizeof(typename T::type_t) < sizeof(typename U::type_t))> {
        typedef typename T::type_t ttype_t;
        typedef typename U::type_t utype_t;
        using type_t = typename type_selector<T, U, (sizeof (ttype_t) < sizeof (utype_t))>::type_t;

        constexpr static const bool areEquallySmall = sizeof(ttype_t) == sizeof(utype_t);
        constexpr static const bool isFirstSmaller = sizeof(ttype_t) < sizeof(utype_t);
        constexpr static const bool isSecondSmaller = sizeof(ttype_t) > sizeof(utype_t);
    };

    template<typename T>
    struct numeric_limits {
        static const size_t digits10 = 0;
    };

    template<>
    struct numeric_limits<uint8_t> {
        static const size_t digits10 = 3;
    };

    template<>
    struct numeric_limits<uint16_t> {
        static const size_t digits10 = 5;
    };

    template<>
    struct numeric_limits<uint32_t> {
        static const size_t digits10 = 10;
    };

    template<>
    struct numeric_limits<uint64_t> {
        static const size_t digits10 = 20;
    };

    enum type_t {

        type_void = 0, type_tinyint, type_shortint, type_int, type_largeint, type_string, type_fixed, type_char, type_restiny, type_resshort, type_resint
    };

    typedef void empty_t;
    typedef uint8_t tinyint_t;
    typedef uint16_t shortint_t;
    typedef uint32_t int_t;
    typedef uint64_t bigint_t;
    typedef char char_t, *str_t;
    typedef const char cchar_t, *cstr_t;
    typedef const char * const cstrc_t;
    typedef double fixed_t;

    typedef uint32_t id_t;
    typedef uint32_t oid_t;
    typedef uint32_t version_t;

// enforce real different types. The typedef's above result in type-clashes!

    struct v2_base_t {
    };

    struct v2_empty_t : public v2_base_t {

        typedef empty_t type_t;
        typedef v2_empty_t v2_unenc_t;
        typedef v2_empty_t v2_copy_t;
        typedef v2_empty_t v2_select_t;
        typedef v2_empty_t v2_compare_t;

        // we do not define dhm_emptykey and dhm_deletedkey to get compile-time errorsif we use this type unintendedly
    };

    struct v2_tinyint_t : public v2_base_t {

        typedef tinyint_t type_t;
        typedef v2_tinyint_t v2_unenc_t;
        typedef v2_unenc_t v2_copy_t;
        typedef v2_unenc_t v2_select_t;
        typedef v2_unenc_t v2_compare_t;

        static const type_t dhm_emptykey;
        static const type_t dhm_deletedkey;
    };

    struct v2_shortint_t : public v2_base_t {

        typedef shortint_t type_t;
        typedef v2_shortint_t v2_unenc_t;
        typedef v2_unenc_t v2_copy_t;
        typedef v2_unenc_t v2_select_t;
        typedef v2_unenc_t v2_compare_t;

        static const type_t dhm_emptykey;
        static const type_t dhm_deletedkey;
    };

    struct v2_int_t : public v2_base_t {

        typedef int_t type_t;
        typedef v2_int_t v2_unenc_t;
        typedef v2_unenc_t v2_copy_t;
        typedef v2_unenc_t v2_select_t;
        typedef v2_unenc_t v2_compare_t;

        static const type_t dhm_emptykey;
        static const type_t dhm_deletedkey;
    };

    struct v2_bigint_t : public v2_base_t {

        typedef bigint_t type_t;
        typedef v2_bigint_t v2_unenc_t;
        typedef v2_unenc_t v2_copy_t;
        typedef v2_unenc_t v2_select_t;
        typedef v2_unenc_t v2_compare_t;

        static const type_t dhm_emptykey;
        static const type_t dhm_deletedkey;
    };

    struct v2_char_t : public v2_base_t {

        typedef char_t type_t;
        typedef v2_char_t v2_unenc_t;
        typedef v2_unenc_t v2_copy_t;
        typedef v2_unenc_t v2_select_t;
        typedef v2_unenc_t v2_compare_t;

        static const type_t dhm_emptykey;
        static const type_t dhm_deletedkey;
    };

    struct v2_str_t : public v2_base_t {

        typedef str_t type_t;
        typedef v2_str_t v2_unenc_t;
        typedef v2_unenc_t v2_copy_t;
        typedef v2_unenc_t v2_select_t;
        typedef v2_int_t v2_compare_t;

        static const type_t dhm_emptykey;
        static const type_t dhm_deletedkey;
    };

    struct v2_fixed_t : public v2_base_t {

        typedef fixed_t type_t;
        typedef v2_fixed_t v2_unenc_t;
        typedef v2_unenc_t v2_copy_t;
        typedef v2_unenc_t v2_select_t;
        typedef v2_unenc_t v2_compare_t;

        static const type_t dhm_emptykey;
        static const type_t dhm_deletedkey;
    };

    struct v2_id_t : public v2_base_t {

        typedef id_t type_t;
        typedef v2_id_t v2_unenc_t;
        typedef v2_unenc_t v2_copy_t;
        typedef v2_unenc_t v2_select_t;
        typedef v2_unenc_t v2_compare_t;

        static const type_t dhm_emptykey;
        static const type_t dhm_deletedkey;
    };

    struct v2_oid_t : public v2_base_t {

        typedef oid_t type_t;
        typedef v2_oid_t v2_unenc_t;
        typedef v2_unenc_t v2_copy_t;
        typedef v2_unenc_t v2_select_t;
        typedef v2_unenc_t v2_compare_t;

        static const type_t dhm_emptykey;
        static const type_t dhm_deletedkey;
    };

    struct v2_void_t : public v2_base_t {

        typedef oid_t type_t;
        typedef v2_void_t v2_unenc_t;
        typedef v2_unenc_t v2_copy_t;
        typedef v2_oid_t v2_select_t;
        typedef v2_oid_t v2_compare_t;

        static const type_t dhm_emptykey;
        static const type_t dhm_deletedkey;
    };

    struct v2_version_t : public v2_base_t {

        typedef version_t type_t;
        typedef v2_version_t v2_unenc_t;
        typedef v2_unenc_t v2_copy_t;
        typedef v2_unenc_t v2_select_t;
        typedef v2_unenc_t v2_compare_t;

        static const type_t dhm_emptykey;
        static const type_t dhm_deletedkey;
    };

    struct v2_size_t : public v2_base_t {

        typedef size_t type_t;
        typedef v2_size_t v2_unenc_t;
        typedef v2_unenc_t v2_copy_t;
        typedef v2_unenc_t v2_select_t;
        typedef v2_unenc_t v2_compare_t;

        static const type_t dhm_emptykey;
        static const type_t dhm_deletedkey;
    };

    extern const typename v2_id_t::type_t ID_INVALID;
    extern const typename v2_oid_t::type_t OID_INVALID;
    extern const typename v2_version_t::type_t VERSION_INVALID;

}

#endif /* V2TYPES_HPP */
