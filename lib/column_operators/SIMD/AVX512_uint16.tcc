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
 * AVX512_uint16.tcc
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
                        inline void pack_right2_uint16(
                                uint16_t * & result,
                                __m512i & a,
                                uint32_t mask) {
                            *result = reinterpret_cast<uint16_t*>(&a)[current];
                            result += (mask >> current) & 0x1;
                            pack_right2_uint16<current + 1>(result, a, mask);
                        }

                        template<>
                        inline void pack_right2_uint16<31>(
                                uint16_t * & result,
                                __m512i & a,
                                uint32_t mask) {
                            *result = reinterpret_cast<uint16_t*>(&a)[31];
                            result += (mask >> 15) & 0x1;
                        }

                    }

                    template<>
                    struct mm512<uint16_t> {

                        typedef uint32_t mask_t;

                        static inline __m512i set1(
                                uint16_t value) {
                            return _mm512_set1_epi16(value);
                        }

                        static inline __m512i set(
                                uint16_t v31,
                                uint16_t v30,
                                uint16_t v29,
                                uint16_t v28,
                                uint16_t v27,
                                uint16_t v26,
                                uint16_t v25,
                                uint16_t v24,
                                uint16_t v23,
                                uint16_t v22,
                                uint16_t v21,
                                uint16_t v20,
                                uint16_t v19,
                                uint16_t v18,
                                uint16_t v17,
                                uint16_t v16,
                                uint16_t v15,
                                uint16_t v14,
                                uint16_t v13,
                                uint16_t v12,
                                uint16_t v11,
                                uint16_t v10,
                                uint16_t v9,
                                uint16_t v8,
                                uint16_t v7,
                                uint16_t v6,
                                uint16_t v5,
                                uint16_t v4,
                                uint16_t v3,
                                uint16_t v2,
                                uint16_t v1,
                                uint16_t v0) {
#ifdef __AVX512BW__
                            return _mm512_set_epi16(v31, v30, v29, v28, v27, v26, v25, v24, v23, v22, v21, v20, v19, v18, v17, v16, v15, v14, v13, v12, v11, v10, v9, v8, v7, v6, v5, v4, v3, v2, v1,
                                    v0);
#else
                            return _mm512_set_epi32((static_cast<uint32_t>(v31) << 16) | static_cast<uint32_t>(v30), (static_cast<uint32_t>(v29) << 16) | static_cast<uint32_t>(v28),
                                    (static_cast<uint32_t>(v27) << 16) | static_cast<uint32_t>(v26), (static_cast<uint32_t>(v25) << 16) | static_cast<uint32_t>(v24),
                                    (static_cast<uint32_t>(v23) << 16) | static_cast<uint32_t>(v22), (static_cast<uint32_t>(v21) << 16) | static_cast<uint32_t>(v20),
                                    (static_cast<uint32_t>(v19) << 16) | static_cast<uint32_t>(v18), (static_cast<uint32_t>(v17) << 16) | static_cast<uint32_t>(v16),
                                    (static_cast<uint32_t>(v15) << 16) | static_cast<uint32_t>(v14), (static_cast<uint32_t>(v13) << 16) | static_cast<uint32_t>(v12),
                                    (static_cast<uint32_t>(v11) << 16) | static_cast<uint32_t>(v10), (static_cast<uint32_t>(v9) << 16) | static_cast<uint32_t>(v8),
                                    (static_cast<uint32_t>(v7) << 16) | static_cast<uint32_t>(v6), (static_cast<uint32_t>(v5) << 16) | static_cast<uint32_t>(v4),
                                    (static_cast<uint32_t>(v3) << 16) | static_cast<uint32_t>(v2), (static_cast<uint32_t>(v1) << 16) | static_cast<uint32_t>(v0));
#endif
                        }

                        static inline __m512i set_inc(
                                uint16_t v0) {
#ifdef __AVX512BW__
                            return _mm512_set_epi16(v0 + 31, v0 + 30, v0 + 29, v0 + 28, v0 + 27, v0 + 26, v0 + 25, v0 + 24, v0 + 23, v0 + 22, v0 + 21, v0 + 20, v0 + 19, v0 + 18, v0 + 17, v0 + 16,
                                    v0 + 15, v0 + 14, v0 + 13, v0 + 12, v0 + 11, v0 + 10, v0 + 9, v0 + 8, v0 + 7, v0 + 6, v0 + 5, v0 + 4, v0 + 3, v0 + 2, v0 + 1, v0);
#else
                            return _mm512_set_epi32(((static_cast<uint32_t>(v0) << 16) + 31) | (static_cast<uint32_t>(v0) + 30),
                                    ((static_cast<uint32_t>(v0) << 16) + 29) | (static_cast<uint32_t>(v0) + 28), ((static_cast<uint32_t>(v0) << 16) + 27) | (static_cast<uint32_t>(v0) + 26),
                                    ((static_cast<uint32_t>(v0) << 16) + 25) | (static_cast<uint32_t>(v0) + 24), ((static_cast<uint32_t>(v0) << 16) + 23) | (static_cast<uint32_t>(v0) + 22),
                                    ((static_cast<uint32_t>(v0) << 16) + 21) | (static_cast<uint32_t>(v0) + 20), ((static_cast<uint32_t>(v0) << 16) + 19) | (static_cast<uint32_t>(v0) + 18),
                                    ((static_cast<uint32_t>(v0) << 16) + 17) | (static_cast<uint32_t>(v0) + 16), ((static_cast<uint32_t>(v0) << 16) + 15) | (static_cast<uint32_t>(v0) + 14),
                                    ((static_cast<uint32_t>(v0) << 16) + 13) | (static_cast<uint32_t>(v0) + 12), ((static_cast<uint32_t>(v0) << 16) + 11) | (static_cast<uint32_t>(v0) + 10),
                                    ((static_cast<uint32_t>(v0) << 16) + 9) | (static_cast<uint32_t>(v0) + 8), ((static_cast<uint32_t>(v0) << 16) + 7) | (static_cast<uint32_t>(v0) + 6),
                                    ((static_cast<uint32_t>(v0) << 16) + 5) | (static_cast<uint32_t>(v0) + 4), ((static_cast<uint32_t>(v0) << 16) + 3) | (static_cast<uint32_t>(v0) + 2),
                                    ((static_cast<uint32_t>(v0) << 16) + 1) | (static_cast<uint32_t>(v0)));
#endif
                        }

                        static inline __m512i set_inc(
                                uint16_t v0,
                                uint16_t inc) {
#ifdef __AVX512BW__
                            return _mm512_set_epi16(v0 + 31 * inc, v0 + 30 * inc, v0 + 29 * inc, v0 + 28 * inc, v0 + 27 * inc, v0 + 26 * inc, v0 + 25 * inc, v0 + 24 * inc, v0 + 23 * inc,
                                    v0 + 22 * inc, v0 + 21 * inc, v0 + 20 * inc, v0 + 19 * inc, v0 + 18 * inc, v0 + 17 * inc, v0 + 16 * inc, v0 + 15 * inc, v0 + 14 * inc, v0 + 13 * inc, v0 + 12 * inc,
                                    v0 + 11 * inc, v0 + 10 * inc, v0 + 9 * inc, v0 + 8 * inc, v0 + 7 * inc, v0 + 6 * inc, v0 + 5 * inc, v0 + 4 * inc, v0 + 3 * inc, v0 + 2 * inc, v0 + inc, v0);
#else
                            return _mm512_set_epi32(((static_cast<uint32_t>(v0) << 16) + 31 * inc) | (static_cast<uint32_t>(v0) + 30 * inc),
                                    ((static_cast<uint32_t>(v0) << 16) + 29 * inc) | (static_cast<uint32_t>(v0) + 28 * inc),
                                    ((static_cast<uint32_t>(v0) << 16) + 27 * inc) | (static_cast<uint32_t>(v0) + 26 * inc),
                                    ((static_cast<uint32_t>(v0) << 16) + 25 * inc) | (static_cast<uint32_t>(v0) + 24 * inc),
                                    ((static_cast<uint32_t>(v0) << 16) + 23 * inc) | (static_cast<uint32_t>(v0) + 22 * inc),
                                    ((static_cast<uint32_t>(v0) << 16) + 21 * inc) | (static_cast<uint32_t>(v0) + 20 * inc),
                                    ((static_cast<uint32_t>(v0) << 16) + 19 * inc) | (static_cast<uint32_t>(v0) + 18 * inc),
                                    ((static_cast<uint32_t>(v0) << 16) + 17 * inc) | (static_cast<uint32_t>(v0) + 16 * inc),
                                    ((static_cast<uint32_t>(v0) << 16) + 15 * inc) | (static_cast<uint32_t>(v0) + 14 * inc),
                                    ((static_cast<uint32_t>(v0) << 16) + 13 * inc) | (static_cast<uint32_t>(v0) + 12 * inc),
                                    ((static_cast<uint32_t>(v0) << 16) + 11 * inc) | (static_cast<uint32_t>(v0) + 10 * inc),
                                    ((static_cast<uint32_t>(v0) << 16) + 9 * inc) | (static_cast<uint32_t>(v0) + 8 * inc),
                                    ((static_cast<uint32_t>(v0) << 16) + 7 * inc) | (static_cast<uint32_t>(v0) + 6 * inc),
                                    ((static_cast<uint32_t>(v0) << 16) + 5 * inc) | (static_cast<uint32_t>(v0) + 4 * inc),
                                    ((static_cast<uint32_t>(v0) << 16) + 3 * inc) | (static_cast<uint32_t>(v0) + 2 * inc),
                                    ((static_cast<uint32_t>(v0) << 16) + 1 * inc) | (static_cast<uint32_t>(v0) + 0 * inc));
#endif
                        }

                        static inline __m512i min(
                                __m512i a,
                                __m512i b) {
                            return _mm512_min_epu16(a, b);
                        }

                        static inline __m512i max(
                                __m512i a,
                                __m512i b) {
                            return _mm512_max_epu16(a, b);
                        }

                        static inline __m512i add(
                                __m512i a,
                                __m512i b) {
                            return _mm512_add_epi16(a, b);
                        }

                        static inline __m512i mullo(
                                __m512i a,
                                __m512i b) {
#ifdef __AVX512BW__
                            return _mm512_mullo_epi16(a, b);
#else
                            throw std::runtime_error("__m512i mullo uint16_t not supported yet");
#endif
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
                                uint16_t * & result,
                                __m512i a,
                                mask_t mask) {
                            Private::pack_right2_uint16(result, a, mask);
                        }

                        static inline void pack_right3(
                                uint16_t * & result,
                                __m512i a,
                                mask_t mask) {
                            typedef mm<__m128i, uint16_t>::mask_t sse_mask_t;
                            auto subMask = static_cast<sse_mask_t>(mask);
                            _mm_storeu_si128(reinterpret_cast<__m128i *>(result), mm<__m128i, uint16_t>::pack_right(_mm512_extracti32x4_epi32(a, 0), subMask));
                            result += __builtin_popcount(subMask);
                            subMask = static_cast<sse_mask_t>(mask >> 8);
                            _mm_storeu_si128(reinterpret_cast<__m128i *>(result), mm<__m128i, uint16_t>::pack_right(_mm512_extracti32x4_epi32(a, 1), subMask));
                            result += __builtin_popcount(subMask);
                            subMask = static_cast<sse_mask_t>(mask >> 16);
                            _mm_storeu_si128(reinterpret_cast<__m128i *>(result), mm<__m128i, uint16_t>::pack_right(_mm512_extracti32x4_epi32(a, 2), subMask));
                            result += __builtin_popcount(subMask);
                            subMask = static_cast<sse_mask_t>(mask >> 24);
                            _mm_storeu_si128(reinterpret_cast<__m128i *>(result), mm<__m128i, uint16_t>::pack_right(_mm512_extracti32x4_epi32(a, 3), subMask));
                            result += __builtin_popcount(subMask);
                        }
                    };

                }
            }
        }
    }
}
