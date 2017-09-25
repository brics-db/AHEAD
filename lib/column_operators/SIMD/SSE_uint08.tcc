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
 * SSE_uint8.tcc
 *
 *  Created on: 25.09.2017
 *      Author: Till Kolditz - Till.Kolditz@gmail.com
 */

#pragma once

#ifndef LIB_COLUMN_OPERATORS_SIMD_SSE_HPP_
#error "This file must only be included by SSE.hpp !"
#endif

namespace ahead {
    namespace bat {
        namespace ops {
            namespace simd {
                namespace sse {

                    template<>
                    struct v2_mm128<uint8_t> {

                        typedef uint16_t mask_t;

                        static inline __m128i set1(
                                uint8_t value) {
                            return _mm_set1_epi8(value);
                        }

                        static inline __m128i set(
                                uint8_t v15,
                                uint8_t v14,
                                uint8_t v13,
                                uint8_t v12,
                                uint8_t v11,
                                uint8_t v10,
                                uint8_t v9,
                                uint8_t v8,
                                uint8_t v7,
                                uint8_t v6,
                                uint8_t v5,
                                uint8_t v4,
                                uint8_t v3,
                                uint8_t v2,
                                uint8_t v1,
                                uint8_t v0) {
                            return _mm_set_epi8(v15, v14, v13, v12, v11, v10, v9, v8, v7, v6, v5, v4, v3, v2, v1, v0);
                        }

                        static inline __m128i set_inc(
                                uint8_t v0) {
                            return _mm_set_epi8(v0 + 15, v0 + 14, v0 + 13, v0 + 12, v0 + 11, v0 + 10, v0 + 9, v0 + 8, v0 + 7, v0 + 6, v0 + 5, v0 + 4, v0 + 3, v0 + 2, v0 + 1, v0);
                        }

                        static inline __m128i set_inc(
                                uint8_t v0,
                                uint16_t inc) {
                            return _mm_set_epi8(v0 + 15 * inc, v0 + 14 * inc, v0 + 13 * inc, v0 + 12 * inc, v0 + 11 * inc, v0 + 10 * inc, v0 + 9 * inc, v0 + 8 * inc, v0 + 7 * inc, v0 + 6 * inc,
                                    v0 + 5 * inc, v0 + 4 * inc, v0 + 3 * inc, v0 + 2 * inc, v0 + inc, v0);
                        }

                        static inline __m128i min(
                                __m128i a,
                                __m128i b) {
                            return _mm_min_epu8(a, b);
                        }

                        static inline __m128i max(
                                __m128i a,
                                __m128i b) {
                            return _mm_max_epu8(a, b);
                        }

                        static inline __m128i add(
                                __m128i a,
                                __m128i b) {
                            return _mm_add_epi8(a, b);
                        }

                        static inline uint8_t sum(
                                __m128i a) {
                            auto mm = _mm_add_epi8(a, _mm_srli_si128(a, 8));
                            mm = _mm_add_epi8(mm, _mm_srli_si128(mm, 4));
                            mm = _mm_add_epi8(mm, _mm_srli_si128(mm, 2));
                            return static_cast<uint16_t>(_mm_extract_epi8(mm, 0));
                        }

                        static inline __m128i pack_right(
                                __m128i a,
                                mask_t mask) {
                            static const uint64_t ALL_ONES = 0xFFFFFFFFFFFFFFFFull;
                            uint64_t shuffleMaskL = SHUFFLE_TABLE_L[static_cast<uint8_t>(mask)];
                            int clzb = __builtin_clzll(~shuffleMaskL); // number of unmatched bytes (if a value matches, the leading bits are zero and the inversion makes it ones, so only full bytes are counted)
                            uint64_t shuffleMaskH = SHUFFLE_TABLE_H[static_cast<uint8_t>(mask >> 8)];
                            return _mm_shuffle_epi8(a, _mm_set_epi64x(((shuffleMaskH >> clzb) | (ALL_ONES << (64 - clzb))), shuffleMaskL & ((shuffleMaskH << (64 - clzb)) | (ALL_ONES >> clzb))));
                        }

                        static inline void pack_right2(
                                uint8_t * & result,
                                __m128i a,
                                mask_t mask) {
                            Private::pack_right2_uint8(result, a, mask);
                        }

                    private:
                        static const uint64_t * const SHUFFLE_TABLE_L;
                        static const uint64_t * const SHUFFLE_TABLE_H;
                    };

                    template<>
                    struct v2_mm128<uint8_t, Greater> {

                        typedef uint16_t mask_t;

                        static inline __m128i cmp(
                                __m128i a,
                                __m128i b) {
                            return _mm_cmplt_epi8(b, a);
                        }

                        static inline mask_t cmp_mask(
                                __m128i a,
                                __m128i b) {
                            return static_cast<mask_t>(_mm_movemask_epi8(cmp(a, b)));
                        }
                    };

                    template<>
                    struct v2_mm128<uint8_t, Greater_equal> {

                        typedef uint16_t mask_t;

                        static inline __m128i cmp(
                                __m128i a,
                                __m128i b) {
                            auto mm = v2_mm128 < uint8_t > ::max(a, b);
                            return _mm_cmpeq_epi8(a, mm);
                        }

                        static inline mask_t cmp_mask(
                                __m128i a,
                                __m128i b) {
                            return static_cast<mask_t>(_mm_movemask_epi8(cmp(a, b)));
                        }
                    };

                    template<>
                    struct v2_mm128<uint8_t, Less> {

                        typedef uint16_t mask_t;

                        static inline __m128i cmp(
                                __m128i a,
                                __m128i b) {
                            return _mm_cmplt_epi8(a, b);
                        }

                        static inline mask_t cmp_mask(
                                __m128i a,
                                __m128i b) {
                            return static_cast<mask_t>(_mm_movemask_epi8(cmp(a, b)));
                        }
                    };

                    template<>
                    struct v2_mm128<uint8_t, Less_equal> {

                        typedef uint16_t mask_t;

                        static inline __m128i cmp(
                                __m128i a,
                                __m128i b) {
                            auto mm = v2_mm128 < uint8_t > ::min(a, b);
                            return _mm_cmpeq_epi8(a, mm);
                        }

                        static inline mask_t cmp_mask(
                                __m128i a,
                                __m128i b) {
                            return static_cast<mask_t>(_mm_movemask_epi8(cmp(a, b)));
                        }
                    };

                    template<>
                    struct v2_mm128<uint8_t, Equal_to> {

                        typedef uint16_t mask_t;

                        static inline __m128i cmp(
                                __m128i a,
                                __m128i b) {
                            return _mm_cmpeq_epi8(a, b);
                        }

                        static inline mask_t cmp_mask(
                                __m128i a,
                                __m128i b) {
                            return static_cast<mask_t>(_mm_movemask_epi8(cmp(a, b)));
                        }
                    };

                    template<>
                    struct v2_mm128<uint8_t, Not_equal_to> {

                        typedef uint16_t mask_t;

                        static inline __m128i cmp(
                                __m128i a,
                                __m128i b) {
                            return _mm_or_si128(_mm_cmplt_epi8(a, b), _mm_cmpgt_epi8(a, b));
                        }

                        static inline mask_t cmp_mask(
                                __m128i a,
                                __m128i b) {
                            return static_cast<mask_t>(_mm_movemask_epi8(cmp(a, b)));
                        }
                    };

                    template<>
                    struct v2_mm128<uint8_t, And> {

                        typedef uint16_t mask_t;

                        static inline __m128i cmp(
                                __m128i a,
                                __m128i b) {
                            return _mm_and_si128(a, b);
                        }

                        static inline mask_t cmp_mask(
                                __m128i a,
                                __m128i b) {
                            return static_cast<mask_t>(_mm_movemask_epi8(cmp(a, b)));
                        }
                    };

                    template<>
                    struct v2_mm128<uint8_t, Or> {

                        typedef uint16_t mask_t;

                        static inline __m128i cmp(
                                __m128i a,
                                __m128i b) {
                            return _mm_or_si128(a, b);
                        }

                        static inline mask_t cmp_mask(
                                __m128i a,
                                __m128i b) {
                            return static_cast<mask_t>(_mm_movemask_epi8(cmp(a, b)));
                        }
                    };

                    template<>
                    struct v2_mm128<uint8_t, Add> {

                        static inline __m128i compute(
                                __m128i a,
                                __m128i b) {
                            return add(a, b);
                        }

                        static inline __m128i add(
                                __m128i a,
                                __m128i b) {
                            return _mm_add_epi8(a, b);
                        }
                    };

                    template<>
                    struct v2_mm128<uint8_t, Sub> {

                        static inline __m128i compute(
                                __m128i a,
                                __m128i b) {
                            return sub(a, b);
                        }

                        static inline __m128i sub(
                                __m128i a,
                                __m128i b) {
                            return _mm_sub_epi8(a, b);
                        }
                    };

                    template<>
                    struct v2_mm128<uint8_t, Mul> {

                        static inline __m128i compute(
                                __m128i a,
                                __m128i b) {
                            return mullo(a, b);
                        }

                        static inline __m128i mullo(
                                __m128i a,
                                __m128i b) {
                            auto mm1 = _mm_shuffle_epi8(_mm_mullo_epi16(_mm_cvtepi8_epi16(a), _mm_cvtepi8_epi16(b)), _mm_set_epi64x(0xFFFFFFFFFFFFFFFFull, 0x0D0C090805040100ull));
                            auto mm2 = _mm_shuffle_epi8(_mm_mullo_epi16(_mm_cvtepi8_epi16(_mm_srli_si128(a, 8)), _mm_cvtepi8_epi16(_mm_srli_si128(b, 8))),
                                    _mm_set_epi64x(0x0D0C090805040100ull, 0xFFFFFFFFFFFFFFFFull));
                            return _mm_and_si128(mm1, mm2);
                        }
                    };

                    template<>
                    struct v2_mm128<uint8_t, Div> {

                        static inline __m128i compute(
                                __m128i a,
                                __m128i b) {
                            return div(a, b);
                        }

                        static inline __m128i div(
                                __m128i a,
                                __m128i b) {
                            auto mm1 = _mm_div_ps(_mm_cvtepi32_ps(_mm_cvtepi8_epi32(a)), _mm_cvtepi32_ps(_mm_cvtepi8_epi32(b)));
                            auto mm2 = _mm_div_ps(_mm_cvtepi32_ps(_mm_cvtepi8_epi32(_mm_srli_si128(a, 4))), _mm_cvtepi32_ps(_mm_cvtepi8_epi32(_mm_srli_si128(b, 4))));
                            auto mm3 = _mm_div_ps(_mm_cvtepi32_ps(_mm_cvtepi8_epi32(_mm_srli_si128(a, 8))), _mm_cvtepi32_ps(_mm_cvtepi8_epi32(_mm_srli_si128(b, 8))));
                            auto mm4 = _mm_div_ps(_mm_cvtepi32_ps(_mm_cvtepi8_epi32(_mm_srli_si128(a, 12))), _mm_cvtepi32_ps(_mm_cvtepi8_epi32(_mm_srli_si128(b, 12))));
                            auto mx1 = _mm_cvtepi32_epi8(_mm_cvtps_epi32(mm1));
                            auto mx2 = _mm_shuffle_epi8(_mm_cvtepi32_epi8(_mm_cvtps_epi32(mm2)), _mm_set_epi64x(0xFFFFFFFFFFFFFFFF, 0x0C080400FFFFFFFF));
                            auto mx3 = _mm_shuffle_epi8(_mm_cvtepi32_epi8(_mm_cvtps_epi32(mm3)), _mm_set_epi64x(0xFFFFFFFF0C080400, 0xFFFFFFFFFFFFFFFF));
                            auto mx4 = _mm_shuffle_epi8(_mm_cvtepi32_epi8(_mm_cvtps_epi32(mm4)), _mm_set_epi64x(0x0C080400FFFFFFFF, 0xFFFFFFFFFFFFFFFF));
                            return _mm_or_si128(_mm_or_si128(mx1, mx2), _mm_or_si128(mx3, mx4));
                        }
                    };

                }
            }
        }
    }
}
