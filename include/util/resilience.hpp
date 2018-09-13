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

namespace ahead {

    typedef uint16_t A_t;
#ifndef RESTINY32
    typedef uint16_t restiny_t, restinyint_t;
#else
    typedef uint32_t restiny_t, restinyint_t;
#endif
    typedef uint32_t resshort_t, resshortint_t;
    typedef uint64_t resint_t;
    typedef uint64_t resbigint_t;
    typedef uint64_t resoid_t;

    struct v2_restiny_t;
    struct v2_resint_t;
    struct v2_resshort_t;
    struct v2_resbigint_t;
    struct v2_resoid_t;
    struct v2_resstr_t;

#define RESOID_INVALID (static_cast<resoid_t>(-1))

    template<typename V2Type, typename T>
    struct ANParameters;

    template<>
    struct ANParameters<v2_restiny_t, uint16_t> {
        typedef std::array<A_t, 8> array_A_t;
        typedef std::array<uint16_t, 8> array_A_inv_t;
        typedef std::array<A_t, 4> array_A_minbfw_t;
        typedef std::array<uint16_t, 4> array_A_minbfw_inv_t;

        static const constexpr array_A_t As = {1, 3, 7, 13, 29, 59, 115, 233};
        static const constexpr array_A_inv_t AInvs = {0x0001, 0xaaab, 0x6db7, 0x4ec5, 0xc235, 0xd8f3, 0xa4bb, 0xd759};
        static const constexpr array_A_minbfw_t AsBFW = {As[0], As[1], As[4], As[7]};
        static const constexpr array_A_minbfw_inv_t AInvsBFW = {AInvs[0], AInvs[1], AInvs[4], AInvs[7]};
    };

    template<>
    struct ANParameters<v2_restiny_t, uint32_t> {
        typedef std::array<A_t, 16> array_A_t;
        typedef std::array<uint32_t, 16> array_A_inv_t;
        typedef std::array<A_t, 7> array_A_minbfw_t;
        typedef std::array<uint32_t, 7> array_A_minbfw_inv_t;

        static const constexpr array_A_t As = {1, 3, 7, 13, 29, 59, 115, 233, 487, 857, 1939, 3813, 7463, 13963, 27247, 55831};
        static const constexpr array_A_inv_t AInvs = {0x00000001, 0xaaaaaaab, 0xb6db6db7, 0xc4ec4ec5, 0x4f72c235, 0xa08ad8f3, 0x61f2a4bb, 0x1fdcd759, 0xb98f81d7, 0x484a14e9, 0xbc452e9b, 0xeecffeed,
                0xfc778297, 0xb9860123, 0x833a348f, 0x4dfffda7};
        static const constexpr array_A_minbfw_t AsBFW = {As[0], As[1], As[4], As[7], As[10], As[13], As[15]};
        static const constexpr array_A_minbfw_inv_t AInvsBFW = {AInvs[0], AInvs[1], AInvs[4], AInvs[7], AInvs[10], AInvs[13], AInvs[15]};
    };

    template<>
    struct ANParameters<v2_resshort_t, uint32_t> {
        typedef std::array<A_t, 16> array_A_t;
        typedef std::array<uint32_t, 16> array_A_inv_t;
        typedef std::array<A_t, 6> array_A_minbfw_t;
        typedef std::array<uint32_t, 6> array_A_minbfw_inv_t;

        static const constexpr array_A_t As = {1, 3, 7, 13, 29, 61, 119, 233, 463, 947, 1939, 3349, 7785, 14781, 28183, 63877};
        static const constexpr array_A_inv_t AInvs = {0x00000001, 0xaaaaaaab, 0xb6db6db7, 0xc4ec4ec5, 0x4f72c235, 0xc10c9715, 0x46fdd947, 0x1fdcd759, 0xab67652f, 0xff30637b, 0xbc452e9b, 0x21b5da3d,
                0x392f51d9, 0x1abdc995, 0xab2da9a7, 0xd142174d};
        static const constexpr array_A_minbfw_t AsBFW = {As[0], As[1], As[5], As[8], As[12], As[15]};
        static const constexpr array_A_minbfw_inv_t AInvsBFW = {AInvs[0], AInvs[1], AInvs[5], AInvs[8], AInvs[12], AInvs[15]};
    };

    template<>
    struct ANParameters<v2_resint_t, uint64_t> {
        typedef std::array<A_t, 16> array_A_t;
        typedef std::array<uint64_t, 16> array_A_inv_t;
        typedef std::array<A_t, 5> array_A_minbfw_t;
        typedef std::array<uint64_t, 5> array_A_minbfw_inv_t;

        static const constexpr std::array<A_t, 16> As = {1, 3, 7, 13, 29, 61, 125, 225, 445, 881, 2029, 3973, 7841, 16089, 32417, 65117};
        static const constexpr std::array<resint_t, 16> AInvs = {1, 0xAAAAAAAAAAAAAAAB, 0x6DB6DB6DB6DB6DB7, 0x4EC4EC4EC4EC4EC5, 0x34F72C234F72C235, 0x4FBCDA3AC10C9715, 0x1CAC083126E978D5,
                0x0FEDCBA987654321, 0x64194FF6CBA64195, 0xC87FDACE4F9E5D91, 0x49AEFF9F19DD6DE5, 0xA6B1742D1ABD914D, 0x3434A14919D34561, 0x227F016A87066169, 0x73B9563A744AE561, 0x3E36D3BC8D9BC5F5};
        static const constexpr std::array<A_t, 5> AsBFW = {As[0], As[1], As[6], As[9], As[14]};
        static const constexpr std::array<resint_t, 5> AInvsBFW = {AInvs[0], AInvs[1], AInvs[6], AInvs[9], AInvs[14]};
    };

    struct v2_anencoded_t {
    };

    struct v2_restiny_t :
            public v2_anencoded_t {

        typedef restiny_t type_t;
        typedef v2_tinyint_t v2_unenc_t;
        typedef v2_restiny_t v2_copy_t;
        typedef v2_restiny_t v2_select_t;
        typedef v2_restiny_t v2_compare_t;
        typedef v2_resshort_t v2_larger_t;

        static const type_t dhm_emptykey;
        static const type_t dhm_deletedkey;

        static const restiny_t UNENC_MIN;
        static const restiny_t UNENC_MAX;
        static const restiny_t UNENC_MAX_U;

        using Params = ANParameters<v2_restiny_t, restiny_t>;
        using array_A_t = typename Params::array_A_t;
        using array_A_inv_t = typename Params::array_A_inv_t;
        using array_A_minbfw_t = typename Params::array_A_minbfw_t;
        using array_A_minbfw_inv_t = typename Params::array_A_minbfw_inv_t;

        static const constexpr array_A_t * As = &Params::As;
        static const constexpr array_A_inv_t * Ainvs = &Params::AInvs;
        static const constexpr array_A_minbfw_t * AsBFW = &Params::AsBFW;
        static const constexpr array_A_minbfw_inv_t * AinvsBFW = &Params::AInvsBFW;
    };
    typedef v2_restiny_t v2_restinyint_t;

    struct v2_resshort_t :
            public v2_anencoded_t {

        typedef resshort_t type_t;
        typedef v2_shortint_t v2_unenc_t;
        typedef v2_resshort_t v2_copy_t;
        typedef v2_resshort_t v2_select_t;
        typedef v2_resshort_t v2_compare_t;
        typedef v2_resint_t v2_larger_t;

        static const type_t dhm_emptykey;
        static const type_t dhm_deletedkey;

        static const resshort_t UNENC_MIN;
        static const resshort_t UNENC_MAX;
        static const resshort_t UNENC_MAX_U;

        using Params = ANParameters<v2_resshort_t, resshort_t>;
        using array_A_t = typename Params::array_A_t;
        using array_A_inv_t = typename Params::array_A_inv_t;
        using array_A_minbfw_t = typename Params::array_A_minbfw_t;
        using array_A_minbfw_inv_t = typename Params::array_A_minbfw_inv_t;

        static const constexpr array_A_t * As = &Params::As;
        static const constexpr array_A_inv_t * Ainvs = &Params::AInvs;
        static const constexpr array_A_minbfw_t * AsBFW = &Params::AsBFW;
        static const constexpr array_A_minbfw_inv_t * AinvsBFW = &Params::AInvsBFW;
    };
    typedef v2_resshort_t v2_resshortint_t;

    struct v2_resint_t :
            public v2_anencoded_t {

        typedef resint_t type_t;
        typedef v2_int_t v2_unenc_t;
        typedef v2_resint_t v2_copy_t;
        typedef v2_resint_t v2_select_t;
        typedef v2_resint_t v2_compare_t;
        typedef v2_resbigint_t v2_larger_t;

        static const type_t dhm_emptykey;
        static const type_t dhm_deletedkey;

        static const resint_t UNENC_MIN;
        static const resint_t UNENC_MAX;
        static const resint_t UNENC_MAX_U;

        using Params = ANParameters<v2_resint_t, resint_t>;
        using array_A_t = typename Params::array_A_t;
        using array_A_inv_t = typename Params::array_A_inv_t;
        using array_A_minbfw_t = typename Params::array_A_minbfw_t;
        using array_A_minbfw_inv_t = typename Params::array_A_minbfw_inv_t;

        static const constexpr array_A_t * As = &Params::As;
        static const constexpr array_A_inv_t * Ainvs = &Params::AInvs;
        static const constexpr array_A_minbfw_t * AsBFW = &Params::AsBFW;
        static const constexpr array_A_minbfw_inv_t * AinvsBFW = &Params::AInvsBFW;
    };

    struct v2_resbigint_t :
            public v2_anencoded_t {

        typedef resbigint_t type_t;
        typedef v2_bigint_t v2_unenc_t;
        typedef v2_resbigint_t v2_copy_t;
        typedef v2_resbigint_t v2_select_t;
        typedef v2_resbigint_t v2_compare_t;
        typedef v2_resbigint_t v2_larger_t;

        static const type_t dhm_emptykey;
        static const type_t dhm_deletedkey;

        static const resbigint_t UNENC_MIN;
        static const resbigint_t UNENC_MAX;
        static const resbigint_t UNENC_MAX_U;

        using Params = ANParameters<v2_resint_t, resint_t>;
        using array_A_t = typename Params::array_A_t;
        using array_A_inv_t = typename Params::array_A_inv_t;
        using array_A_minbfw_t = typename Params::array_A_minbfw_t;
        using array_A_minbfw_inv_t = typename Params::array_A_minbfw_inv_t;

        static const constexpr array_A_t * As = &Params::As;
        static const constexpr array_A_inv_t * Ainvs = &Params::AInvs;
        static const constexpr array_A_minbfw_t * AsBFW = &Params::AsBFW;
        static const constexpr array_A_minbfw_inv_t * AinvsBFW = &Params::AInvsBFW;
    };

    struct v2_resoid_t :
            public v2_anencoded_t {

        typedef resoid_t type_t;
        typedef v2_oid_t v2_unenc_t;
        typedef v2_resoid_t v2_copy_t;
        typedef v2_resoid_t v2_select_t;
        typedef v2_resoid_t v2_compare_t;
        typedef v2_resbigint_t v2_larger_t;

        static const type_t dhm_emptykey;
        static const type_t dhm_deletedkey;

        static const resoid_t UNENC_MIN;
        static const resoid_t UNENC_MAX;
        static const resoid_t UNENC_MAX_U;

        using Params = ANParameters<v2_resint_t, resint_t>;
        using array_A_t = typename Params::array_A_t;
        using array_A_inv_t = typename Params::array_A_inv_t;
        using array_A_minbfw_t = typename Params::array_A_minbfw_t;
        using array_A_minbfw_inv_t = typename Params::array_A_minbfw_inv_t;

        static const constexpr array_A_t * As = &Params::As;
        static const constexpr array_A_inv_t * Ainvs = &Params::AInvs;
        static const constexpr array_A_minbfw_t * AsBFW = &Params::AsBFW;
        static const constexpr array_A_minbfw_inv_t * AinvsBFW = &Params::AInvsBFW;
    };

    struct v2_resstr_t :
            public v2_anencoded_t {

        typedef str_t type_t;
        typedef v2_str_t v2_unenc_t;
        typedef v2_resstr_t v2_copy_t;
        typedef v2_resstr_t v2_select_t;
        typedef v2_resstr_t v2_compare_t;

        static str_t dhm_emptykey;
        static str_t dhm_deletedkey;

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
        static cstrc_t TYPENAME;
    };

    template<>
    struct TypeMap<v2_shortint_t> {

        typedef v2_shortint_t v2_base_t;
        typedef v2_resshort_t v2_encoded_t;
        typedef v2_shortint_t v2_actual_t;
        static cstrc_t TYPENAME;
    };

    template<>
    struct TypeMap<v2_int_t> {

        typedef v2_int_t v2_base_t;
        typedef v2_resint_t v2_encoded_t;
        typedef v2_int_t v2_actual_t;
        static cstrc_t TYPENAME;
    };

    template<>
    struct TypeMap<v2_bigint_t> {

        typedef v2_bigint_t v2_base_t;
        typedef v2_resbigint_t v2_encoded_t;
        typedef v2_bigint_t v2_actual_t;
        static cstrc_t TYPENAME;
    };

    template<>
    struct TypeMap<v2_oid_t> {

        typedef v2_oid_t v2_base_t;
        typedef v2_resoid_t v2_encoded_t;
        typedef v2_oid_t v2_actual_t;
        static cstrc_t TYPENAME;
    };

    template<>
    struct TypeMap<v2_void_t> {

        typedef v2_void_t v2_base_t;
        typedef v2_resoid_t v2_encoded_t;
        typedef v2_oid_t v2_actual_t;
        static cstrc_t TYPENAME;
    };

    template<>
    struct TypeMap<v2_str_t> {

        typedef v2_str_t v2_base_t;
        typedef v2_str_t v2_encoded_t;
        typedef v2_str_t v2_actual_t;
        static cstrc_t TYPENAME;
    };

    template<>
    struct TypeMap<v2_restiny_t> {

        typedef v2_tinyint_t v2_base_t;
        typedef v2_restiny_t v2_encoded_t;
        typedef v2_restiny_t v2_actual_t;
        static cstrc_t TYPENAME;
    };

    template<>
    struct TypeMap<v2_resshort_t> {

        typedef v2_shortint_t v2_base_t;
        typedef v2_resshort_t v2_encoded_t;
        typedef v2_resshort_t v2_actual_t;
        static cstrc_t TYPENAME;
    };

    template<>
    struct TypeMap<v2_resint_t> {

        typedef v2_int_t v2_base_t;
        typedef v2_resint_t v2_encoded_t;
        typedef v2_resint_t v2_actual_t;
        static cstrc_t TYPENAME;
    };

    template<>
    struct TypeMap<v2_resbigint_t> {

        typedef v2_bigint_t v2_base_t;
        typedef v2_resbigint_t v2_encoded_t;
        typedef v2_resbigint_t v2_actual_t;
        static cstrc_t TYPENAME;
    };

    template<>
    struct TypeMap<v2_resoid_t> {

        typedef v2_oid_t v2_base_t;
        typedef v2_resoid_t v2_encoded_t;
        typedef v2_resoid_t v2_actual_t;
        static cstrc_t TYPENAME;
    };

    template<>
    struct TypeMap<v2_resstr_t> {

        typedef v2_str_t v2_base_t;
        typedef v2_resstr_t v2_encoded_t;
        typedef v2_str_t v2_actual_t;
        static cstrc_t TYPENAME;
    };

    template<>
    struct TypeMap<v2_fixed_t> {

        typedef v2_fixed_t v2_base_t;
        typedef v2_fixed_t v2_encoded_t;
        typedef v2_fixed_t v2_actual_t;
        static cstrc_t TYPENAME;
    };

    template<>
    struct TypeMap<v2_char_t> {

        typedef v2_char_t v2_base_t;
        typedef v2_char_t v2_encoded_t;
        typedef v2_char_t v2_actual_t;
        static cstrc_t TYPENAME;
    };

    template<>
    struct TypeMap<v2_id_t> {

        typedef v2_id_t v2_base_t;
        typedef v2_id_t v2_encoded_t;
        typedef v2_id_t v2_actual_t;
        static cstrc_t TYPENAME;
    };

    template<>
    struct TypeMap<v2_size_t> {

        typedef v2_size_t v2_base_t;
        typedef v2_size_t v2_encoded_t;
        typedef v2_size_t v2_actual_t;
        static cstrc_t TYPENAME;
    };

    template<typename V2Type, bool isV2BaseType>
    struct ANParametersSelector0;

    template<typename V2Type>
    struct ANParametersSelector0<V2Type, false> {

        typedef typename V2Type::type_t type_t;

        static const constexpr type_t UNENC_MIN = V2Type::A_UNENC_MIN;
        static const constexpr type_t UNENC_MAX = V2Type::A_UNENC_MAX;
        static const constexpr type_t UNENC_MAX_U = V2Type::A_UNENC_MAX_U;

        static const constexpr std::array<A_t, 16> * As = V2Type::As;
        static const constexpr std::array<type_t, 16> * Ainvs = V2Type::Ainvs;
        static const constexpr std::array<A_t, 16> * AsBFW = V2Type::AsBFW;
        static const constexpr std::array<type_t, 16> * AinvsBFW = V2Type::AinvsBFW;
    };

    template<typename V2Type>
    struct ANParametersSelector0<V2Type, true> {

        typedef typename V2Type::type_t type_t;

        static const constexpr type_t UNENC_MIN = std::numeric_limits<std::make_signed<type_t>>::min();
        static const constexpr type_t UNENC_MAX = std::numeric_limits<std::make_signed<type_t>>::max();
        static const constexpr type_t UNENC_MAX_U = std::numeric_limits<std::make_unsigned<type_t>>::max();

        static const constexpr std::array<A_t, 16> * As = nullptr;
        static const constexpr std::array<type_t, 16> * Ainvs = nullptr;
        static const constexpr std::array<A_t, 16> * AsBFW = nullptr;
        static const constexpr std::array<type_t, 16> * AinvsBFW = nullptr;
    };

    template<typename V2Type>
    struct ANParametersSelector {

        typedef typename V2Type::type_t type_t;
        typedef ANParametersSelector0<V2Type, std::is_base_of<v2_base_t, V2Type>::value> selector_t;

        static const constexpr type_t UNENC_MIN = selector_t::UNENC_MIN;
        static const constexpr type_t UNENC_MAX = selector_t::UNENC_MAX;
        static const constexpr type_t UNENC_MAX_U = selector_t::UNENC_MAX_U;

        static const constexpr std::array<A_t, 16> * As = selector_t::As;
        static const constexpr std::array<type_t, 16> * Ainvs = selector_t::Ainvs;
    };

    template<typename T>
    T ext_euclidean(
            T b0,
            size_t codewidth) {
        T a0(1);
        a0 <<= codewidth;
        T a[20], b[20], q[20], r[20], s[20], t[20]; // 16 values is enough for 16-bit A's, let's add some space for larger A's
        uint8_t aI = 1, bI = 1, qI = 0, rI = 0, sI = 1, tI = 1;
        a[0] = a0;
        b[0] = b0;
        s[0] = 0;
        t[0] = 0;
        ssize_t i = 0;
        do {
            q[qI++] = a[i] / b[i];
            r[rI++] = a[i] % b[i];
            a[aI++] = b[i];
            b[bI++] = r[i];
            s[sI++] = 0;
            t[tI++] = 0;
        } while (b[++i] > 0);
        s[i] = 1;
        t[i] = 0;

        for (ssize_t j = i; j > 0; --j) {
            s[j - 1] = t[j];
            t[j - 1] = s[j] - q[j - 1] * t[j];
        }
        T result = ((b0 * t[0]) % a0);
        result += result < 0 ? a0 : 0;
        if (result == 1) {
            return t[0];
        } else {
            return 0;
        }
    }

}

#endif /* RESILIENCE_HPP */
