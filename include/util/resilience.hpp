// Copyright (c) 2016 Till Kolditz
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

/* 
 * File:   resilience.hpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 29. Juli 2016, 12:08
 */

#ifndef RESILIENCE_HPP
#define RESILIENCE_HPP

#include <cstdint>

#include <util/v2types.hpp>
#include <column_storage/Bat.h>
#include <column_storage/ColumnBat.h>
#include <column_storage/TempBat.h>

typedef uint16_t restiny_t;
typedef uint32_t resshort_t;
typedef uint64_t resint_t;
typedef uint64_t resbigint_t;
typedef uint64_t resoid_t;

#define RESOID_INVALID (static_cast<resoid_t>(-1))

struct v2_anencoded_t {
};

struct v2_restiny_t : public v2_anencoded_t {
    typedef restiny_t type_t;
    typedef v2_tinyint_t unenc_v2_t;
    typedef v2_restiny_t v2_copy_t;
    typedef v2_restiny_t v2_select_t;
    typedef v2_restiny_t v2_compare_t;

    static const type_t dhm_emptykey;
    static const type_t dhm_deletedkey;

    static const restiny_t A;
    static const restiny_t A_INV;
    static const restiny_t A_UNENC_MIN;
    static const restiny_t A_UNENC_MAX;
    static const restiny_t A_UNENC_MAX_U;
};

struct v2_resshort_t : public v2_anencoded_t {
    typedef resshort_t type_t;
    typedef v2_shortint_t unenc_v2_t;
    typedef v2_resshort_t v2_copy_t;
    typedef v2_resshort_t v2_select_t;
    typedef v2_resshort_t v2_compare_t;

    static const type_t dhm_emptykey;
    static const type_t dhm_deletedkey;

    static const resshort_t A;
    static const resshort_t A_INV;
    static const resshort_t A_UNENC_MIN;
    static const resshort_t A_UNENC_MAX;
    static const resshort_t A_UNENC_MAX_U;
};

struct v2_resint_t : public v2_anencoded_t {
    typedef resint_t type_t;
    typedef v2_int_t unenc_v2_t;
    typedef v2_resint_t v2_copy_t;
    typedef v2_resint_t v2_select_t;
    typedef v2_resint_t v2_compare_t;

    static const type_t dhm_emptykey;
    static const type_t dhm_deletedkey;

    static const resint_t A;
    static const resint_t A_INV;
    static const resint_t A_UNENC_MIN;
    static const resint_t A_UNENC_MAX;
    static const resint_t A_UNENC_MAX_U;
};

struct v2_resbigint_t : public v2_anencoded_t {
    typedef resbigint_t type_t;
    typedef v2_bigint_t unenc_v2_t;
    typedef v2_resbigint_t v2_copy_t;
    typedef v2_resbigint_t v2_select_t;
    typedef v2_resbigint_t v2_compare_t;

    static const type_t dhm_emptykey;
    static const type_t dhm_deletedkey;

    static const resbigint_t A;
    static const resbigint_t A_INV;
    static const resbigint_t A_UNENC_MIN;
    static const resbigint_t A_UNENC_MAX;
    static const resbigint_t A_UNENC_MAX_U;
};

struct v2_resoid_t : public v2_anencoded_t {
    typedef resoid_t type_t;
    typedef v2_oid_t unenc_v2_t;
    typedef v2_resoid_t v2_copy_t;
    typedef v2_resoid_t v2_select_t;
    typedef v2_resoid_t v2_compare_t;

    static const type_t dhm_emptykey;
    static const type_t dhm_deletedkey;

    static const resoid_t A;
    static const resoid_t A_INV;
    static const resoid_t A_UNENC_MIN;
    static const resoid_t A_UNENC_MAX;
    static const resoid_t A_UNENC_MAX_U;
};

struct v2_resstr_t : public v2_anencoded_t {
    typedef str_t type_t;
    typedef v2_str_t unenc_v2_t;
    typedef v2_str_t v2_copy_t;
    typedef v2_str_t v2_select_t;
    typedef v2_str_t v2_compare_t;

    static cstr_t dhm_emptykey;
    static cstr_t dhm_deletedkey;

    static str_t A;
    static str_t A_INV;
    static str_t A_UNENC_MIN;
    static str_t A_UNENC_MAX;
    static str_t A_UNENC_MAX_U;
};

// Fast-Forward declare Bat, ColumnBat, TempBat types
typedef Bat<v2_void_t, v2_restiny_t> restiny_bat_t;
typedef Bat<v2_void_t, v2_resshort_t> resshort_bat_t;
typedef Bat<v2_void_t, v2_resint_t> resint_bat_t;
typedef Bat<v2_void_t, v2_resoid_t> resoid_bat_t;
typedef Bat<v2_void_t, v2_resstr_t> resstr_bat_t;

typedef ColumnBat<v2_void_t, v2_restiny_t> restiny_colbat_t;
typedef ColumnBat<v2_void_t, v2_resshort_t> resshort_colbat_t;
typedef ColumnBat<v2_void_t, v2_resint_t> resint_colbat_t;
typedef ColumnBat<v2_void_t, v2_resoid_t> resoid_colbat_t;
typedef ColumnBat<v2_void_t, v2_resstr_t> resstr_colbat_t;

typedef TempBat<v2_void_t, v2_restiny_t> restiny_tmpbat_t;
typedef TempBat<v2_void_t, v2_resshort_t> resshort_tmpbat_t;
typedef TempBat<v2_void_t, v2_resint_t> resint_tmpbat_t;
typedef TempBat<v2_void_t, v2_resoid_t> resoid_tmpbat_t;
typedef TempBat<v2_void_t, v2_resstr_t> resstr_tmpbat_t;

template<typename Base>
struct TypeMap;

template<>
struct TypeMap<v2_tinyint_t> {
    typedef v2_tinyint_t v2_base_t;
    typedef v2_restiny_t v2_encoded_t;
    typedef v2_tinyint_t v2_actual_t;
    static cstr_t TYPENAME;
};

template<>
struct TypeMap<v2_shortint_t> {
    typedef v2_shortint_t v2_base_t;
    typedef v2_resshort_t v2_encoded_t;
    typedef v2_shortint_t v2_actual_t;
    static cstr_t TYPENAME;
};

template<>
struct TypeMap<v2_int_t> {
    typedef v2_int_t v2_base_t;
    typedef v2_resint_t v2_encoded_t;
    typedef v2_int_t v2_actual_t;
    static cstr_t TYPENAME;
};

template<>
struct TypeMap<v2_bigint_t> {
    typedef v2_bigint_t v2_base_t;
    typedef v2_resbigint_t v2_encoded_t;
    typedef v2_bigint_t v2_actual_t;
    static cstr_t TYPENAME;
};

template<>
struct TypeMap<v2_oid_t> {
    typedef v2_oid_t v2_base_t;
    typedef v2_resoid_t v2_encoded_t;
    typedef v2_oid_t v2_actual_t;
    static cstr_t TYPENAME;
};

template<>
struct TypeMap<v2_void_t> {
    typedef v2_void_t v2_base_t;
    typedef v2_resoid_t v2_encoded_t;
    typedef v2_oid_t v2_actual_t;
    static cstr_t TYPENAME;
};

template<>
struct TypeMap<v2_str_t> {
    typedef v2_str_t v2_base_t;
    typedef v2_resstr_t v2_encoded_t;
    typedef v2_str_t v2_actual_t;
    static cstr_t TYPENAME;
};

template<>
struct TypeMap<v2_restiny_t> {
    typedef v2_tinyint_t v2_base_t;
    typedef v2_restiny_t v2_encoded_t;
    typedef v2_restiny_t v2_actual_t;
    static cstr_t TYPENAME;
};

template<>
struct TypeMap<v2_resshort_t> {
    typedef v2_shortint_t v2_base_t;
    typedef v2_resshort_t v2_encoded_t;
    typedef v2_resshort_t v2_actual_t;
    static cstr_t TYPENAME;
};

template<>
struct TypeMap<v2_resint_t> {
    typedef v2_int_t v2_base_t;
    typedef v2_resint_t v2_encoded_t;
    typedef v2_resint_t v2_actual_t;
    static cstr_t TYPENAME;
};

template<>
struct TypeMap<v2_resbigint_t> {
    typedef v2_bigint_t v2_base_t;
    typedef v2_resbigint_t v2_encoded_t;
    typedef v2_resbigint_t v2_actual_t;
    static cstr_t TYPENAME;
};

template<>
struct TypeMap<v2_resoid_t> {
    typedef v2_oid_t v2_base_t;
    typedef v2_resoid_t v2_encoded_t;
    typedef v2_resoid_t v2_actual_t;
    static cstr_t TYPENAME;
};

template<>
struct TypeMap<v2_resstr_t> {
    typedef v2_str_t v2_base_t;
    typedef v2_resstr_t v2_encoded_t;
    typedef v2_str_t v2_actual_t;
    static cstr_t TYPENAME;
};

#endif /* RESILIENCE_HPP */
