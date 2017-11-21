// Copyright 2017 Till Kolditz
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
 * AVX512_uint32.tcc
 *
 *  Created on: 20.11.2017
 *      Author: Till Kolditz - Till.Kolditz@gmail.com
 */

#pragma once

#ifndef LIB_COLUMN_OPERATORS_SIMD_AVX512_HPP_
#error "This file must only be included by AVX512.hpp !"
#endif

namespace ahead {
    namespace bat {
        namespace ops {
            namespace simd {
                namespace avx512 {

                    namespace Private {

                        template<size_t current = 0>
                        inline void pack_right2_uint32(
                                uint32_t * & result,
                                __m512i & a,
                                uint16_t mask) {
                            *result = reinterpret_cast<uint32_t*>(&a)[current];
                            result += (mask >> current) & 0x1;
                            pack_right2_uint32<current + 1>(result, a, mask);
                        }

                        template<>
                        inline void pack_right2_uint32<15>(
                                uint32_t * & result,
                                __m512i & a,
                                uint16_t mask) {
                            *result = reinterpret_cast<uint32_t*>(&a)[15];
                            result += (mask >> 7) & 0x1;
                        }

                    }

                    template<>
                    struct mm512<uint32_t> {

                        typedef uint16_t mask_t;

                        static inline __m512i set1(
                                uint32_t value) {
                            return _mm512_set1_epi32(value);
                        }

                        static inline __m512i set(
                                uint32_t v15,
                                uint32_t v14,
                                uint32_t v13,
                                uint32_t v12,
                                uint32_t v11,
                                uint32_t v10,
                                uint32_t v9,
                                uint32_t v8,
                                uint32_t v7,
                                uint32_t v6,
                                uint32_t v5,
                                uint32_t v4,
                                uint32_t v3,
                                uint32_t v2,
                                uint32_t v1,
                                uint32_t v0) {
                            return _mm512_set_epi32(v15, v14, v13, v12, v11, v10, v9, v8, v7, v6, v5, v4, v3, v2, v1, v0);
                        }

                        static inline __m512i set_inc(
                                uint32_t v0) {
                            return _mm512_set_epi32(v0 + 15, v0 + 14, v0 + 13, v0 + 12, v0 + 11, v0 + 10, v0 + 9, v0 + 8, v0 + 7, v0 + 6, v0 + 5, v0 + 4, v0 + 3, v0 + 2, v0 + 1, v0);
                        }

                        static inline __m512i set_inc(
                                uint32_t v0,
                                uint32_t inc) {
                            return _mm512_set_epi32(v0 + 15 * inc, v0 + 14 * inc, v0 + 13 * inc, v0 + 12 * inc, v0 + 11 * inc, v0 + 10 * inc, v0 + 9 * inc, v0 + 8 * inc, v0 + 7 * inc, v0 + 6 * inc,
                                    v0 + 5 * inc, v0 + 4 * inc, v0 + 3 * inc, v0 + 2 * inc, v0 + inc, v0);
                        }

                        static inline __m512i min(
                                __m512i a,
                                __m512i b) {
                            return _mm512_min_epu32(a, b);
                        }

                        static inline __m512i max(
                                __m512i a,
                                __m512i b) {
                            return _mm512_max_epu32(a, b);
                        }

                        static inline __m512i add(
                                __m512i a,
                                __m512i b) {
                            return _mm512_add_epi32(a, b);
                        }

                        static inline __m512i mullo(
                                __m512i a,
                                __m512i b) {
                            return _mm512_mullo_epi32(a, b);
                        }

                        static inline __m512i geq(
                                __m512i a,
                                __m512i b) {
#define MASK(idx) ((mask & idx) ? 0xFFFFFFFF : 0)
                            auto mask = geq_mask(a, b);
                            return set(MASK(0x8000), MASK(0x4000), MASK(0x2000), MASK(0x1000), MASK(0x0800), MASK(0x0400), MASK(0x0200), MASK(0x0100), MASK(0x0080), MASK(0x0040), MASK(0x0020),
                                    MASK(0x0010), MASK(0x0008), MASK(0x0004), MASK(0x0002), MASK(0x0001));
#undef MASK
                        }

                        static inline uint16_t geq_mask(
                                __m512i a,
                                __m512i b) {
                            return _mm512_cmpge_epu32_mask(a, b);
                        }

                        static inline uint8_t sum(
                                __m512i a) {
                            throw std::runtime_error("__m512i sum uint16_t not supported yet");
                        }

                        static inline __m512i pack_right(
                                __m512i a,
                                mask_t mask) {
                            throw std::runtime_error("__m512i pack_right uint16_t not supported yet");
                        }

                        static inline void pack_right2(
                                uint32_t * & result,
                                __m512i a,
                                mask_t mask) {
                            Private::pack_right2_uint32(result, a, mask);
                        }

                        static inline void pack_right3(
                                uint32_t * & result,
                                __m512i a,
                                mask_t mask) {
                            typedef mm<__m128i, uint32_t>::mask_t sse_mask_t;
                            auto subMask = static_cast<sse_mask_t>(mask) & 0xF;
                            _mm_storeu_si128(reinterpret_cast<__m128i *>(result), mm<__m128i, uint32_t>::pack_right(_mm512_extracti32x4_epi32(a, 0), subMask));
                            result += __builtin_popcount(subMask);
                            subMask = static_cast<sse_mask_t>(mask >> 4) & 0xF;
                            _mm_storeu_si128(reinterpret_cast<__m128i *>(result), mm<__m128i, uint32_t>::pack_right(_mm512_extracti32x4_epi32(a, 1), subMask));
                            result += __builtin_popcount(subMask);
                            subMask = static_cast<sse_mask_t>(mask >> 8) & 0xF;
                            _mm_storeu_si128(reinterpret_cast<__m128i *>(result), mm<__m128i, uint32_t>::pack_right(_mm512_extracti32x4_epi32(a, 2), subMask));
                            result += __builtin_popcount(subMask);
                            subMask = static_cast<sse_mask_t>(mask >> 12) & 0xF;
                            _mm_storeu_si128(reinterpret_cast<__m128i *>(result), mm<__m128i, uint32_t>::pack_right(_mm512_extracti32x4_epi32(a, 3), subMask));
                            result += __builtin_popcount(subMask);
                        }
                    };

                }
            }
        }
    }
}
