/*
 * Copyright 2017 Till Kolditz <till.kolditz@gmail.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* 
 * File:   SSECMP.hpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 24. Februar 2017, 14:07
 */

#ifndef SSECMP_HPP
#define SSECMP_HPP

#include <immintrin.h>

#include "SSE.hpp"
#include <column_operators/functors.hpp>

namespace ahead {
    namespace bat {
        namespace ops {
            namespace sse {

                namespace Private {

                    inline uint8_t v2_mm128_compact_mask_uint16_t(
                            uint16_t && mask) {
                        mask = mask & 0x5555;
                        mask = ((mask >> 1) | mask) & 0x3333;
                        mask = ((mask >> 2) | mask) & 0x0F0F;
                        mask = ((mask >> 4) | mask) & 0x00FF;
                        return static_cast<uint8_t>(mask);
                    }

                }

                template<typename T, template<typename > class Op>
                struct v2_mm128_cmp;

                template<>
                struct v2_mm128_cmp<uint8_t, std::greater> {

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
                struct v2_mm128_cmp<uint8_t, std::greater_equal> {

                    typedef uint16_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        auto mm = v2_mm128<uint8_t>::max(a, b);
                        return _mm_cmpeq_epi8(a, mm);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_epi8(cmp(a, b)));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint8_t, std::less> {

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
                struct v2_mm128_cmp<uint8_t, std::less_equal> {

                    typedef uint16_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        auto mm = v2_mm128<uint8_t>::min(a, b);
                        return _mm_cmpeq_epi8(a, mm);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_epi8(cmp(a, b)));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint8_t, std::equal_to> {

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
                struct v2_mm128_cmp<uint8_t, std::not_equal_to> {

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
                struct v2_mm128_cmp<uint8_t, ahead::bat::ops::AND> {

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
                struct v2_mm128_cmp<uint8_t, ahead::bat::ops::OR> {

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
                struct v2_mm128_cmp<uint16_t, std::greater> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_cmplt_epi16(b, a);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return Private::v2_mm128_compact_mask_uint16_t(static_cast<uint16_t>(_mm_movemask_epi8(cmp(a, b))));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint16_t, std::greater_equal> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        auto mm = v2_mm128<uint16_t>::max(a, b);
                        return _mm_cmpeq_epi16(a, mm);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return Private::v2_mm128_compact_mask_uint16_t(static_cast<uint16_t>(_mm_movemask_epi8(cmp(a, b))));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint16_t, std::less> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_cmplt_epi16(a, b);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return Private::v2_mm128_compact_mask_uint16_t(static_cast<uint16_t>(_mm_movemask_epi8(cmp(a, b))));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint16_t, std::less_equal> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        auto mm = v2_mm128<uint16_t>::min(a, b);
                        return _mm_cmpeq_epi16(a, mm);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return Private::v2_mm128_compact_mask_uint16_t(static_cast<uint16_t>(_mm_movemask_epi8(cmp(a, b))));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint16_t, std::equal_to> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_cmpeq_epi16(a, b);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return Private::v2_mm128_compact_mask_uint16_t(static_cast<uint16_t>(_mm_movemask_epi8(cmp(a, b))));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint16_t, std::not_equal_to> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_or_si128(_mm_cmplt_epi16(a, b), _mm_cmpgt_epi16(a, b));
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return Private::v2_mm128_compact_mask_uint16_t(static_cast<uint16_t>(_mm_movemask_epi8(cmp(a, b))));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint16_t, ahead::bat::ops::AND> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_and_si128(a, b);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return Private::v2_mm128_compact_mask_uint16_t(static_cast<uint16_t>(_mm_movemask_epi8(cmp(a, b))));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint16_t, ahead::bat::ops::OR> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_or_si128(a, b);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return Private::v2_mm128_compact_mask_uint16_t(static_cast<uint16_t>(_mm_movemask_epi8(cmp(a, b))));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint32_t, std::equal_to> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_cmpeq_epi32(a, b);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_ps(_mm_castsi128_ps(cmp(a, b))));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint32_t, std::not_equal_to> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_or_si128(_mm_cmplt_epi32(a, b), _mm_cmpgt_epi32(a, b));
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_ps(_mm_castsi128_ps(cmp(a, b))));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint32_t, std::greater_equal> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_cmpeq_epi32(a, v2_mm128<uint32_t>::max(a, b));
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_ps(_mm_castsi128_ps(cmp(a, b))));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint32_t, std::greater> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_cmpgt_epi32(a, b);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_ps(_mm_castsi128_ps(cmp(a, b))));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint32_t, std::less_equal> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_cmpeq_epi32(a, v2_mm128<uint32_t>::min(a, b));
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_ps(_mm_castsi128_ps(cmp(a, b))));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint32_t, std::less> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_cmpgt_epi32(a, b);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_ps(_mm_castsi128_ps(cmp(a, b))));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint32_t, ahead::bat::ops::AND> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_and_si128(a, b);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_ps(_mm_castsi128_ps(cmp(a, b))));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint32_t, ahead::bat::ops::OR> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_or_si128(a, b);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_ps(_mm_castsi128_ps(cmp(a, b))));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint64_t, std::greater> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_cmpgt_epi64(a, b);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_pd(_mm_castsi128_pd(cmp(a, b))));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint64_t, std::greater_equal> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        auto mm = v2_mm128<uint64_t>::max(a, b);
                        return _mm_cmpeq_epi64(a, mm);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_pd(_mm_castsi128_pd(cmp(a, b))));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint64_t, std::less> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_cmpgt_epi64(b, a);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_pd(_mm_castsi128_pd(cmp(a, b))));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint64_t, std::less_equal> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        auto mm = v2_mm128<uint64_t>::min(a, b);
                        return _mm_cmpeq_epi64(a, mm);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_pd(_mm_castsi128_pd(cmp(a, b))));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint64_t, std::equal_to> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_cmpeq_epi64(a, b);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_pd(_mm_castsi128_pd(cmp(a, b))));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint64_t, std::not_equal_to> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_or_si128(_mm_cmpgt_epi64(a, b), _mm_cmpgt_epi64(b, a));
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_pd(_mm_castsi128_pd(cmp(a, b))));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint64_t, ahead::bat::ops::AND> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_and_si128(a, b);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_pd(_mm_castsi128_pd(cmp(a, b))));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint64_t, ahead::bat::ops::OR> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_or_si128(a, b);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_pd(_mm_castsi128_pd(cmp(a, b))));
                    }
                };

            }
        }
    }
}

#endif /* SSECMP_HPP */
