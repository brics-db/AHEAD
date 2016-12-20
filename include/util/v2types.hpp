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

/* META MACRO intended for generating type names */
#define MKT(...) VFUNC(MKT, __VA_ARGS__)
#define MKT1(BASE) CONCAT(BASE, _t)
#define MKT2(BASE, SUFFIX) MKT1(CONCAT(BASE, SUFFIX))
#define MKT3(PREFIX, BASE, SUFFIX) CONCAT(PREFIX, CONCAT(BASE, SUFFIX))

// typedef the larger of the types

namespace v2 {

    template<typename T, typename U, bool>
    struct larger_type0 {

        typedef T type_t;
    };

    template<typename T, typename U>
    struct larger_type0<T, U, false> {

        typedef U type_t;
    };

    template<typename T, typename U>
    struct larger_type : public larger_type0<T, U, (sizeof (T) >= sizeof (U))> {

        using type_t = typename larger_type0<T, U, (sizeof (T) >= sizeof (U))>::type_t;

        constexpr static const bool areEquallyLarge = sizeof (T) == sizeof (U);
        constexpr static const bool isFirstLarger = sizeof (T) > sizeof (U);
        constexpr static const bool isSecondLarger = sizeof (T) < sizeof (U);
    };
}

enum type_t {

    type_void = 0, type_tinyint, type_shortint, type_int, type_largeint, type_string, type_fixed, type_char, type_restiny, type_resshort, type_resint
};

typedef uint8_t tinyint_t;
typedef uint16_t shortint_t;
typedef uint32_t int_t;
typedef uint64_t bigint_t;
typedef char char_t, *str_t;
typedef const char cchar_t, *cstr_t;
typedef double fixed_t;

typedef uint32_t id_t;
typedef uint32_t oid_t;
typedef uint32_t version_t;

// enforce real different types. The typedef's above result in type-clashes!

struct v2_base_t {

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

#endif /* V2TYPES_HPP */
