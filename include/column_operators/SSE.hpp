// Copyright (c) 2017 Till Kolditz
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
 * File:   SSE.hpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 23. Februar 2017, 22:43
 */

#ifndef SSE_HPP
#define SSE_HPP

#include <immintrin.h>

namespace v2 {
    namespace bat {
        namespace ops {

            template<typename A>
            struct v2_mm128;

            template<>
            struct v2_mm128<uint16_t> {

                static inline __m128i
                set1 (uint16_t value) {
                    return _mm_set1_epi16(value);
                }

                static inline __m128i
                set (uint16_t v7, uint16_t v6, uint16_t v5, uint16_t v4, uint16_t v3, uint16_t v2, uint16_t v1, uint16_t v0) {
                    return _mm_set_epi16(v7, v6, v5, v4, v3, v2, v1, v0);
                }

                static inline __m128i
                min (__m128i & a, __m128i & b) {
                    return _mm_min_epu16(a, b);
                }

                static inline __m128i
                max (__m128i & a, __m128i & b) {
                    return _mm_max_epu16(a, b);
                }

                static inline __m128i
                add (__m128i a, __m128i b) {
                    return _mm_add_epi16(a, b);
                }

                static inline __m128i
                mullo (__m128i & a, __m128i & b) {
                    return _mm_mullo_epi16(a, b);
                }

                static inline __m128i
                geq (__m128i & a, __m128i & b) {
                    auto mm = max(a, b);
                    return _mm_cmpeq_epi16(a, mm);
                }

                static inline uint8_t
                geq_mask (__m128i & a, __m128i & b) {
                    auto mask = static_cast<uint16_t>(_mm_movemask_epi8(geq(a, b)));
                    mask = mask & 0x5555;
                    mask = ((mask >> 1) | mask) & 0x3333;
                    mask = ((mask >> 2) | mask) & 0x0F0F;
                    mask = ((mask >> 4) | mask) & 0x00FF;
                    return static_cast<uint8_t>(mask);
                }

                static inline uint16_t
                sum (__m128i & a) {
                    auto mm = _mm_add_epi16(a, _mm_srli_si128(a, 64));
                    mm = _mm_add_epi16(mm, _mm_srli_si128(mm, 32));
                    mm = _mm_add_epi16(mm, _mm_srli_si128(mm, 16));
                    return static_cast<uint16_t>(_mm_extract_epi16(mm, 0));
                }
            };

            template<>
            struct v2_mm128<uint32_t> {

                static inline __m128i
                set1 (uint32_t value) {
                    return _mm_set1_epi32(value);
                }

                static inline __m128i
                set (uint32_t v3, uint32_t v2, uint32_t v1, uint32_t v0) {
                    return _mm_set_epi32(v3, v2, v1, v0);
                }

                static inline __m128i
                min (__m128i & a, __m128i & b) {
                    return _mm_min_epu32(a, b);
                }

                static inline __m128i
                max (__m128i & a, __m128i & b) {
                    return _mm_max_epu32(a, b);
                }

                static inline __m128i
                add (__m128i a, __m128i b) {
                    return _mm_add_epi32(a, b);
                }

                static inline __m128i
                mullo (__m128i & a, __m128i & b) {
                    return _mm_mullo_epi32(a, b);
                }

                static inline __m128i
                geq (__m128i & a, __m128i & b) {
                    auto mm = max(a, b);
                    return _mm_cmpeq_epi32(a, mm);
                }

                static inline uint8_t
                geq_mask (__m128i & a, __m128i & b) {
                    return static_cast<uint8_t>(_mm_movemask_ps(_mm_castsi128_ps(geq(a, b))));
                }

                static inline uint32_t
                sum (__m128i & a) {
                    auto mm = _mm_add_epi32(a, _mm_srli_si128(a, 64));
                    mm = _mm_add_epi32(mm, _mm_srli_si128(mm, 32));
                    return static_cast<uint32_t>(_mm_extract_epi32(mm, 0));
                }
            };

            template<>
            struct v2_mm128<uint64_t> {

                static inline __m128i
                set1 (uint64_t value) {
                    return _mm_set1_epi64x(value);
                }

                static inline __m128i
                set (uint64_t v1, uint64_t v0) {
                    return _mm_set_epi64x(v1, v0);
                }

                static inline __m128i
                min (__m128i & a, __m128i & b) {
                    return _mm_set_epi64x(std::min(static_cast<uint64_t>(_mm_extract_epi64(a, 1)), static_cast<uint64_t>(_mm_extract_epi64(b, 1))), std::min(static_cast<uint64_t>(_mm_extract_epi64(a, 0)), static_cast<uint64_t>(_mm_extract_epi64(b, 0))));
                }

                static inline __m128i
                max (__m128i & a, __m128i & b) {
                    return _mm_set_epi64x(std::max(static_cast<uint64_t>(_mm_extract_epi64(a, 1)), static_cast<uint64_t>(_mm_extract_epi64(b, 1))), std::max(static_cast<uint64_t>(_mm_extract_epi64(a, 0)), static_cast<uint64_t>(_mm_extract_epi64(b, 0))));
                }

                static inline __m128i
                add (__m128i a, __m128i b) {
                    return _mm_add_epi64(a, b);
                }

                static inline __m128i
                mullo (__m128i & a, __m128i & b) {
                    return _mm_set_epi64x(_mm_extract_epi64(a, 1) * _mm_extract_epi64(b, 1), _mm_extract_epi64(a, 0) * _mm_extract_epi64(b, 0));
                }

                static inline __m128i
                geq (__m128i & a, __m128i & b) {
                    auto mm = max(a, b);
                    return _mm_cmpeq_epi64(a, mm);
                }

                static inline uint8_t
                geq_mask (__m128i & a, __m128i & b) {
                    return static_cast<uint8_t>(_mm_movemask_pd(_mm_castsi128_pd(geq(a, b))));
                }

                static inline uint64_t
                sum (__m128i & a) {
                    return static_cast<uint64_t>(_mm_extract_epi64(a, 0)) + static_cast<uint64_t>(_mm_extract_epi64(a, 1));
                }
            };

            template<typename A, typename B, typename R>
            struct v2_mm128_mullo_add;

            template<>
            struct v2_mm128_mullo_add<uint16_t, uint16_t, uint16_t> {

                inline __m128i operator() (__m128i & a, __m128i & b, size_t & incA, size_t & incB) {
                    incA = incB = 1;
                    return v2_mm128<uint16_t>::mullo(a, b);
                }
            };

            template<>
            struct v2_mm128_mullo_add<uint16_t, uint16_t, uint32_t> {

                inline __m128i operator() (__m128i & a, __m128i & b, size_t & incA, size_t & incB) {
                    incA = incB = 1;
                    auto mm = _mm_mullo_epi32(_mm_cvtepi16_epi32(a), _mm_cvtepi16_epi32(b));
                    mm = _mm_add_epi32(mm, _mm_mullo_epi32(_mm_cvtepi16_epi32(_mm_srli_si128(a, 64)), _mm_cvtepi16_epi32(_mm_srli_si128(b, 64))));
                    return mm;
                }
            };

            template<>
            struct v2_mm128_mullo_add<uint16_t, uint32_t, uint32_t> {

                inline __m128i operator() (__m128i & a, __m128i & b, size_t & incA, size_t & incB) {
                    incA = 1;
                    incB = 2;
                    auto mm = _mm_mullo_epi32(_mm_cvtepi16_epi32(a), *((&b) + 1));
                    mm = _mm_add_epi32(mm, _mm_mullo_epi32(_mm_cvtepi16_epi32(_mm_srli_si128(a, 64)), b));
                    return mm;
                }
            };

            template<>
            struct v2_mm128_mullo_add<uint32_t, uint16_t, uint32_t> {

                inline __m128i operator() (__m128i & a, __m128i & b, size_t & incA, size_t & incB) {
                    return v2_mm128_mullo_add<uint16_t, uint32_t, uint32_t>()(b, a, incB, incA);
                }
            };

            template<>
            struct v2_mm128_mullo_add<uint16_t, uint32_t, uint64_t> {

                inline __m128i operator() (__m128i & a, __m128i & b, size_t & incA, size_t & incB) {
                    incA = 1;
                    incB = 2;
                    auto mm = _mm_mul_epi32(_mm_cvtepi16_epi32(a), *((&b) + 1));
                    mm = _mm_add_epi64(mm, _mm_mul_epi32(_mm_cvtepi16_epi32(_mm_srli_si128(a, 64)), b));
                    return mm;
                }
            };

            template<>
            struct v2_mm128_mullo_add<uint32_t, uint16_t, uint64_t> {

                inline __m128i operator() (__m128i & a, __m128i & b, size_t & incA, size_t & incB) {
                    return v2_mm128_mullo_add<uint16_t, uint32_t, uint64_t>()(b, a, incB, incA);
                }
            };

            template<>
            struct v2_mm128_mullo_add<uint16_t, uint64_t, uint64_t> {

                inline __m128i operator() (__m128i & a, __m128i & b, size_t & incA, size_t & incB) {
                    incA += 1;
                    incB += 4;
                    auto r0 = static_cast<uint64_t>(_mm_extract_epi16(a, 0)) * _mm_extract_epi64(b, 0);
                    auto r1 = static_cast<uint64_t>(_mm_extract_epi16(a, 1)) * _mm_extract_epi64(b, 1);
                    r0 += static_cast<uint64_t>(_mm_extract_epi16(a, 2)) * _mm_extract_epi64(*((&b) + 1), 0);
                    r1 += static_cast<uint64_t>(_mm_extract_epi16(a, 3)) * _mm_extract_epi64(*((&b) + 1), 1);
                    r0 += static_cast<uint64_t>(_mm_extract_epi16(a, 4)) * _mm_extract_epi64(*((&b) + 2), 0);
                    r1 += static_cast<uint64_t>(_mm_extract_epi16(a, 5)) * _mm_extract_epi64(*((&b) + 2), 1);
                    r0 += static_cast<uint64_t>(_mm_extract_epi16(a, 6)) * _mm_extract_epi64(*((&b) + 3), 0);
                    r1 += static_cast<uint64_t>(_mm_extract_epi16(a, 7)) * _mm_extract_epi64(*((&b) + 3), 1);
                    return _mm_set_epi64x(r1, r0);
                }
            };

            template<>
            struct v2_mm128_mullo_add<uint64_t, uint16_t, uint64_t> {

                inline __m128i operator() (__m128i & a, __m128i & b, size_t & incA, size_t & incB) {
                    return v2_mm128_mullo_add<uint16_t, uint64_t, uint64_t>()(b, a, incB, incA);
                }
            };

            template<>
            struct v2_mm128_mullo_add<uint32_t, uint32_t, uint32_t> {

                inline __m128i operator() (__m128i & a, __m128i & b, size_t & incA, size_t & incB) {
                    incA = incB = 1;
                    return v2_mm128<uint32_t>::mullo(a, b);
                }
            };

            template<>
            struct v2_mm128_mullo_add<uint32_t, uint32_t, uint64_t> {

                inline __m128i operator() (__m128i & a, __m128i & b, size_t & incA, size_t & incB) {
                    incA = incB = 1;
                    auto mm = _mm_mul_epu32(a, b);
                    return _mm_add_epi32(mm, _mm_mul_epu32(_mm_srli_si128(a, 64), _mm_srli_si128(b, 64)));
                }
            };

            template<>
            struct v2_mm128_mullo_add<uint64_t, uint64_t, uint64_t> {

                inline __m128i operator() (__m128i & a, __m128i & b, size_t & incA, size_t & incB) {
                    incA = incB = 1;
                    return v2_mm128<uint64_t>::mullo(a, b);
                }
            };

            template<typename A, size_t firstA, typename B, size_t firstB, typename R>
            struct v2_mm128_mullo;

            template<size_t firstA, size_t firstB>
            struct v2_mm128_mullo<uint16_t, firstA, uint64_t, firstB, uint64_t> {

                inline __m128i operator() (__m128i & a, __m128i & b) {
                    auto r0 = static_cast<uint64_t>(_mm_extract_epi16(a, firstA)) * _mm_extract_epi64(b, firstB);
                    auto r1 = static_cast<uint64_t>(_mm_extract_epi16(a, firstA + 1)) * _mm_extract_epi64(b, firstB + 1);
                    return _mm_set_epi64x(r1, r0);
                }
            };

            template<size_t firstA, size_t firstB>
            struct v2_mm128_mullo<uint64_t, firstA, uint16_t, firstB, uint64_t> {

                inline __m128i operator() (__m128i & a, __m128i & b) {
                    return v2_mm128_mullo<uint16_t, firstB, uint64_t, firstA, uint64_t>()(b, a);
                }
            };

            template<typename T>
            void
            v2_mm128_AN_detect (__m128i & mmDec, __m128i & mmCol, __m128i & mmInv, __m128i & mmDMax, std::vector<bool> * vec, size_t pos) {
                mmDec = v2_mm128<T>::mullo(mmCol, mmInv);
                uint8_t maskGT = v2_mm128<T>::geq_mask(mmDec, mmDMax);
                if (maskGT) {
                    uint8_t test = 1;
                    for (size_t k = 0; k < (sizeof (__m128i) / sizeof (T)); ++k, test <<= 1) {
                        if (maskGT & test) {
                            (*vec)[pos + k] = true;
                        }
                    }
                }
            }

        }
    }
}

#endif /* SSE_HPP */
