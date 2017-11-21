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
 * AVX512_uint64.tcc
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
                        inline void pack_right2_uint64(
                                uint64_t * & result,
                                __m512i & a,
                                uint8_t mask) {
                            *result = reinterpret_cast<uint64_t*>(&a)[current];
                            result += (mask >> current) & 0x1;
                            pack_right2_uint64<current + 1>(result, a, mask);
                        }

                        template<>
                        inline void pack_right2_uint64<7>(
                                uint64_t * & result,
                                __m512i & a,
                                uint8_t mask) {
                            *result = reinterpret_cast<uint64_t*>(&a)[7];
                            result += (mask >> 3) & 0x1;
                        }
                    }

                    template<>
                    struct mm512<uint64_t> {

                        typedef uint8_t mask_t;

                        static inline __m512i set1(
                                uint64_t value) {
                            return _mm512_set1_epi64(value);
                        }

                        static inline __m512i set(
                                uint64_t v7,
                                uint64_t v6,
                                uint64_t v5,
                                uint64_t v4,
                                uint64_t v3,
                                uint64_t v2,
                                uint64_t v1,
                                uint64_t v0) {
                            return _mm512_set_epi64(v7, v6, v5, v4, v3, v2, v1, v0);
                        }

                        static inline __m512i set_inc(
                                uint64_t v0) {
                            return _mm512_set_epi64(v0 + 7, v0 + 6, v0 + 5, v0 + 4, v0 + 3, v0 + 2, v0 + 1, v0);
                        }

                        static inline __m512i set_inc(
                                uint64_t v0,
                                uint64_t inc) {
                            return _mm512_set_epi64(v0 + 7 * inc, v0 + 6 * inc, v0 + 5 * inc, v0 + 4 * inc, v0 + 3 * inc, v0 + 2 * inc, v0 + inc, v0);
                        }

                        static inline __m512i min(
                                __m512i a,
                                __m512i b) {
                            return _mm512_min_epi64(a, b);
                        }

                        static inline __m512i max(
                                __m512i a,
                                __m512i b) {
                            return _mm512_max_epi64(a, b);
                        }

                        static inline __m512i add(
                                __m512i a,
                                __m512i b) {
                            return _mm512_add_epi64(a, b);
                        }

                        static inline __m512i mullo(
                                __m512i a,
                                __m512i b) {
                            return _mm512_mullo_epi64(a, b);
                        }

                        static inline __m512i geq(
                                __m512i a,
                                __m512i b) {
#define MASK(idx) ((mask & idx) ? 0xFFFFFFFF : 0)
                            __mmask8 mask = geq_mask(a, b);
                            const uint64_t ONES = 0xFFFFFFFFFFFFFFFFull;
                            return set(MASK(0x80), MASK(0x40), MASK(0x20), MASK(0x10), MASK(0x08), MASK(0x04), MASK(0x02), MASK(0x01));
#undef MASK
                        }

                        static inline uint8_t geq_mask(
                                __m512i a,
                                __m512i b) {
                            return _mm512_cmpge_epu64_mask(a, b);
                        }

                        static inline uint64_t sum(
                                __m512i a) {
                            auto mm = _mm512_add_epi64(a, _mm512_shuffle_i64x2(a, a, 0xA1));
                            auto mm2 = _mm_add_epi64(_mm512_extracti64x2_epi64(mm, 0x2), _mm512_extracti64x2_epi64(mm, 0x0));
                            return static_cast<uint64_t>(_mm_extract_epi64(mm2, 1)) + static_cast<uint64_t>(_mm_extract_epi64(mm2, 0));
                        }

                        static inline __m512i pack_right(
                                __m512i a,
                                mask_t mask) {
                            throw std::runtime_error("__m512i pack_right uint16_t not supported yet");
                        }

                        static inline void pack_right2(
                                uint64_t * & result,
                                __m512i a,
                                mask_t mask) {
                            Private::pack_right2_uint64(result, a, mask);
                        }

                        static inline void pack_right3(
                                uint64_t * & result,
                                __m512i a,
                                mask_t mask) {
                            typedef mm<__m128i, uint64_t>::mask_t sse_mask_t;
                            auto subMask = static_cast<sse_mask_t>(mask) & 0x3;
                            _mm_storeu_si128(reinterpret_cast<__m128i *>(result), mm<__m128i, uint64_t>::pack_right(_mm512_extracti32x4_epi32(a, 0), subMask));
                            result += __builtin_popcount(subMask);
                            subMask = static_cast<sse_mask_t>(mask >> 2) & 0x3;
                            _mm_storeu_si128(reinterpret_cast<__m128i *>(result), mm<__m128i, uint64_t>::pack_right(_mm512_extracti32x4_epi32(a, 1), subMask));
                            result += __builtin_popcount(subMask);
                            subMask = static_cast<sse_mask_t>(mask >> 4) & 0x3;
                            _mm_storeu_si128(reinterpret_cast<__m128i *>(result), mm<__m128i, uint64_t>::pack_right(_mm512_extracti32x4_epi32(a, 2), subMask));
                            result += __builtin_popcount(subMask);
                            subMask = static_cast<sse_mask_t>(mask >> 6) & 0x3;
                            _mm_storeu_si128(reinterpret_cast<__m128i *>(result), mm<__m128i, uint64_t>::pack_right(_mm512_extracti32x4_epi32(a, 3), subMask));
                            result += __builtin_popcount(subMask);
                        }
                    };

                }
            }
        }
    }
}
