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
#include <array>
#include <memory>
#include <limits>
#include <vector>
#include <type_traits>

#include <util/v2types.hpp>

typedef uint16_t restiny_t;
typedef uint32_t resshort_t;
typedef uint64_t resint_t;
typedef uint64_t resbigint_t;
typedef uint64_t resoid_t;

#define RESOID_INVALID (static_cast<resoid_t>(-1))

struct ANParameters {

    static constexpr const std::array<uint16_t, 16> Atiny = {1, 3, 7, 13, 29, 59, 115, 233, 233, 233, 233, 233, 233, 233, 233, 233}; //, 487, 857, 1939, 3813, 7463, 13963, 27247, 55831};
    static constexpr const std::array<restiny_t, 16> AtinyInv = {0x0001, 0xaaab, 0x6db7, 0x4ec5, 0xc235, 0xd8f3, 0xa4bb, 0xd759, 0xd759, 0xd759, 0xd759, 0xd759, 0xd759, 0xd759, 0xd759, 0xd759};
    static constexpr const std::array<uint16_t, 16> Ashort = {1, 3, 7, 13, 29, 61, 119, 233, 463, 947, 1939, 3349, 7785, 14781, 28183, 63877};
    static constexpr const std::array<resshort_t, 16> AshortInv = {0x00000001, 0xaaaaaaab, 0xb6db6db7, 0xc4ec4ec5, 0x4f72c235, 0xc10c9715, 0x46fdd947, 0x1fdcd759, 0xab67652f, 0xff30637b, 0xbc452e9b, 0x21b5da3d, 0x392f51d9, 0x1abdc995, 0xab2da9a7, 0xd142174d};
    static constexpr const std::array<uint16_t, 16> Aint = {1, 3, 7, 15, 21, 55, 125, 225, 445, 881, 2029, 3565, 7947, 16041, 28691, 64311};
    static constexpr const std::array<resint_t, 16> AintInv = {1, 0xAAAAAAAAAAAAAAAB, 0x6DB6DB6DB6DB6DB7, 0xEEEEEEEEEEEEEEEF, 0xCF3CF3CF3CF3CF3D, 0x6FB586FB586FB587, 0x1CAC083126E978D5, 0xFEDCBA987654321, 0x64194FF6CBA64195, 0xC87FDACE4F9E5D91, 0x49AEFF9F19DD6DE5, 0xBF4CC39BC11857E5, 0xFDD779BC079A34A3, 0xC94EE2C7649F4599, 0xBC4B7C5655ECDA1B, 0xAA86FFFEFB1FAA87};
};

struct v2_anencoded_t {

};

struct v2_restiny_t : public v2_anencoded_t {

    typedef restiny_t type_t;
    typedef v2_tinyint_t v2_unenc_t;
    typedef v2_restiny_t v2_copy_t;
    typedef v2_restiny_t v2_select_t;
    typedef v2_restiny_t v2_compare_t;

    static const type_t dhm_emptykey;
    static const type_t dhm_deletedkey;

    static const restiny_t UNENC_MIN;
    static const restiny_t UNENC_MAX;
    static const restiny_t UNENC_MAX_U;

    static constexpr const std::array<uint16_t, 16> * As = &ANParameters::Atiny;
    static constexpr const std::array<restiny_t, 16> * Ainvs = &ANParameters::AtinyInv;
};

struct v2_resshort_t : public v2_anencoded_t {

    typedef resshort_t type_t;
    typedef v2_shortint_t v2_unenc_t;
    typedef v2_resshort_t v2_copy_t;
    typedef v2_resshort_t v2_select_t;
    typedef v2_resshort_t v2_compare_t;

    static const type_t dhm_emptykey;
    static const type_t dhm_deletedkey;

    static const resshort_t UNENC_MIN;
    static const resshort_t UNENC_MAX;
    static const resshort_t UNENC_MAX_U;

    static constexpr const std::array<uint16_t, 16> * As = &ANParameters::Ashort;
    static constexpr const std::array<resshort_t, 16> * Ainvs = &ANParameters::AshortInv;
};

struct v2_resint_t : public v2_anencoded_t {

    typedef resint_t type_t;
    typedef v2_int_t v2_unenc_t;
    typedef v2_resint_t v2_copy_t;
    typedef v2_resint_t v2_select_t;
    typedef v2_resint_t v2_compare_t;

    static const type_t dhm_emptykey;
    static const type_t dhm_deletedkey;

    static const resint_t UNENC_MIN;
    static const resint_t UNENC_MAX;
    static const resint_t UNENC_MAX_U;

    static constexpr const std::array<uint16_t, 16> * As = &ANParameters::Aint;
    static constexpr const std::array<resint_t, 16> * Ainvs = &ANParameters::AintInv;
};

struct v2_resbigint_t : public v2_anencoded_t {

    typedef resbigint_t type_t;
    typedef v2_bigint_t v2_unenc_t;
    typedef v2_resbigint_t v2_copy_t;
    typedef v2_resbigint_t v2_select_t;
    typedef v2_resbigint_t v2_compare_t;

    static const type_t dhm_emptykey;
    static const type_t dhm_deletedkey;

    static const resbigint_t UNENC_MIN;
    static const resbigint_t UNENC_MAX;
    static const resbigint_t UNENC_MAX_U;

    static constexpr const std::array<uint16_t, 16> * As = &ANParameters::Aint;
    static constexpr const std::array<resbigint_t, 16> * Ainvs = &ANParameters::AintInv;
};

struct v2_resoid_t : public v2_anencoded_t {

    typedef resoid_t type_t;
    typedef v2_oid_t v2_unenc_t;
    typedef v2_resoid_t v2_copy_t;
    typedef v2_resoid_t v2_select_t;
    typedef v2_resoid_t v2_compare_t;

    static const type_t dhm_emptykey;
    static const type_t dhm_deletedkey;

    static const resoid_t UNENC_MIN;
    static const resoid_t UNENC_MAX;
    static const resoid_t UNENC_MAX_U;

    static constexpr const std::array<uint16_t, 16> * As = &ANParameters::Aint;
    static constexpr const std::array<resoid_t, 16> * Ainvs = &ANParameters::AintInv;
};

struct v2_resstr_t : public v2_anencoded_t {

    typedef str_t type_t;
    typedef v2_str_t v2_unenc_t;
    typedef v2_str_t v2_copy_t;
    typedef v2_str_t v2_select_t;
    typedef v2_str_t v2_compare_t;

    static cstr_t dhm_emptykey;
    static cstr_t dhm_deletedkey;

    static str_t UNENC_MIN;
    static str_t UNENC_MAX;
    static str_t UNENC_MAX_U;
};

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

template<typename V2Type, bool isV2BaseType>
struct ANParametersSelector0 {

    typedef typename V2Type::type_t type_t;

    static constexpr const type_t UNENC_MIN = V2Type::A_UNENC_MIN;
    static constexpr const type_t UNENC_MAX = V2Type::A_UNENC_MAX;
    static constexpr const type_t UNENC_MAX_U = V2Type::A_UNENC_MAX_U;

    static constexpr const std::array<uint16_t, 16> * As = V2Type::As;
    static constexpr const std::array<type_t, 16> * Ainvs = V2Type::Ainvs;
};

template<typename V2Type>
struct ANParametersSelector0<V2Type, true> {

    typedef typename V2Type::type_t type_t;

    static constexpr const type_t UNENC_MIN = std::numeric_limits<std::make_signed<type_t>>::min ();
    static constexpr const type_t UNENC_MAX = std::numeric_limits<std::make_signed<type_t>>::max ();
    static constexpr const type_t UNENC_MAX_U = std::numeric_limits<std::make_unsigned<type_t>>::max ();

    static constexpr const std::array<uint16_t, 16> * As = nullptr;
    static constexpr const std::array<type_t, 16> * Ainvs = nullptr;
};

template<typename V2Type>
struct ANParametersSelector {

    typedef typename V2Type::type_t type_t;
    typedef ANParametersSelector0<V2Type, std::is_base_of<v2_base_t, V2Type>::value> selector_t;

    static constexpr const type_t UNENC_MIN = selector_t::UNENC_MIN;
    static constexpr const type_t UNENC_MAX = selector_t::UNENC_MAX;
    static constexpr const type_t UNENC_MAX_U = selector_t::UNENC_MAX_U;

    static constexpr const std::array<uint16_t, 16> * As = selector_t::As;
    static constexpr const std::array<type_t, 16> * Ainvs = selector_t::Ainvs;
};

template<typename T>
T
ext_euclidean (T b0, size_t codewidth) {
    T a0(1);
    a0 <<= codewidth;
    std::vector<T> a, b, q, r, s, t;
    a.reserve(8);
    b.reserve(8);
    q.reserve(8);
    r.reserve(8);
    s.reserve(8);
    t.reserve(8);
    a.push_back(a0), b.push_back(b0), s.push_back(T(0)), t.push_back(T(0));
    size_t i = 0;
    do {
        q.push_back(a[i] / b[i]);
        r.push_back(a[i] % b[i]);
        a.push_back(b[i]);
        b.push_back(r[i]);
        s.push_back(0);
        t.push_back(0);
    } while (b[++i] > 0);
    s[i] = 1;
    t[i] = 0;

    for (size_t j = i; j > 0; --j) {
        s[j - 1] = t[j];
        t[j - 1] = s[j] - q[j - 1] * t[j];
    }

    T result = ((b0 * t.front()) % a0);
    result += result < 0 ? a0 : 0;
    if (result == 1) {
        return t.front();
    } else {
        return 0;
    }
}

#endif /* RESILIENCE_HPP */
