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

#include <algorithm>
#include <cstdint>
#include <immintrin.h>

namespace ahead {
    namespace bat {
        namespace ops {
            namespace sse {

                template<typename T>
                struct v2_mm128;

                namespace Private {
                    template<size_t current = 0>
                    inline void pack_right2_uint8(
                            uint8_t * & result,
                            __m128i & a,
                            uint16_t mask) {
                        *result = _mm_extract_epi8(a, current);
                        result += (mask >> current) & 0x1;
                        pack_right2_uint8<current + 1>(result, a, mask);
                    }

                    template<>
                    inline void pack_right2_uint8<15>(
                            uint8_t * & result,
                            __m128i & a,
                            uint16_t mask) {
                        *result = _mm_extract_epi8(a, 15);
                        result += (mask >> 15) & 0x1;
                    }

                    template<size_t current = 0>
                    inline void pack_right2_uint16(
                            uint16_t * & result,
                            __m128i & a,
                            uint8_t mask) {
                        *result = _mm_extract_epi16(a, current);
                        result += (mask >> current) & 0x1;
                        pack_right2_uint16<current + 1>(result, a, mask);
                    }

                    template<>
                    inline void pack_right2_uint16<7>(
                            uint16_t * & result,
                            __m128i & a,
                            uint8_t mask) {
                        *result = _mm_extract_epi16(a, 7);
                        result += (mask >> 7) & 0x1;
                    }

                    template<size_t current = 0>
                    inline void pack_right2_uint32(
                            uint32_t * & result,
                            __m128i & a,
                            uint8_t mask) {
                        *result = _mm_extract_epi32(a, current);
                        result += (mask >> current) & 0x1;
                        pack_right2_uint32<current + 1>(result, a, mask);
                    }

                    template<>
                    inline void pack_right2_uint32<3>(
                            uint32_t * & result,
                            __m128i & a,
                            uint8_t mask) {
                        *result = _mm_extract_epi32(a, 3);
                        result += (mask >> 3) & 0x1;
                    }

                    template<size_t current = 0>
                    inline void pack_right2_uint64(
                            uint64_t * & result,
                            __m128i & a,
                            uint8_t mask) {
                        *result = _mm_extract_epi64(a, current);
                        result += (mask >> current) & 0x1;
                        pack_right2_uint64<current + 1>(result, a, mask);
                    }

                    template<>
                    inline void pack_right2_uint64<1>(
                            uint64_t * & result,
                            __m128i & a,
                            uint8_t mask) {
                        *result = _mm_extract_epi64(a, 1);
                        result += (mask >> 1) & 0x1;
                    }
                }

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

                    static inline __m128i mullo(
                            __m128i a,
                            __m128i b) {
                        auto mm1 = _mm_shuffle_epi8(_mm_mullo_epi16(_mm_cvtepi8_epi16(a), _mm_cvtepi8_epi16(b)), _mm_set_epi64x(0xFFFFFFFFFFFFFFFFull, 0x0D0C090805040100ull));
                        auto mm2 = _mm_shuffle_epi8(_mm_mullo_epi16(_mm_cvtepi8_epi16(_mm_srli_si128(a, 8)), _mm_cvtepi8_epi16(_mm_srli_si128(b, 8))),
                                _mm_set_epi64x(0x0D0C090805040100ull, 0xFFFFFFFFFFFFFFFFull));
                        return _mm_and_si128(mm1, mm2);
                    }

                    static inline uint8_t sum(
                            __m128i a) {
                        auto mm = _mm_add_epi8(a, _mm_srli_si128(a, 8));
                        mm = _mm_add_epi8(mm, _mm_srli_si128(mm, 4));
                        mm = _mm_add_epi8(mm, _mm_srli_si128(mm, 2));
                        return static_cast<uint16_t>(_mm_extract_epi8(mm, 0));
                    }

                    static inline void pack_right2(
                            uint8_t * & result,
                            __m128i a,
                            mask_t mask) {
                        if (mask) {
                            Private::pack_right2_uint8(result, a, mask);
                        }
                    }
                };

                template<>
                struct v2_mm128<uint16_t> {

                    typedef uint8_t mask_t;

                    static inline __m128i set1(
                            uint16_t value) {
                        return _mm_set1_epi16(value);
                    }

                    static inline __m128i set(
                            uint16_t v7,
                            uint16_t v6,
                            uint16_t v5,
                            uint16_t v4,
                            uint16_t v3,
                            uint16_t v2,
                            uint16_t v1,
                            uint16_t v0) {
                        return _mm_set_epi16(v7, v6, v5, v4, v3, v2, v1, v0);
                    }

                    static inline __m128i set_inc(
                            uint16_t v0) {
                        return _mm_set_epi16(v0 + 7, v0 + 6, v0 + 5, v0 + 4, v0 + 3, v0 + 2, v0 + 1, v0);
                    }

                    static inline __m128i set_inc(
                            uint16_t v0,
                            uint16_t inc) {
                        return _mm_set_epi16(v0 + 7 * inc, v0 + 6 * inc, v0 + 5 * inc, v0 + 4 * inc, v0 + 3 * inc, v0 + 2 * inc, v0 + inc, v0);
                    }

                    static inline __m128i min(
                            __m128i a,
                            __m128i b) {
                        return _mm_min_epu16(a, b);
                    }

                    static inline __m128i max(
                            __m128i a,
                            __m128i b) {
                        return _mm_max_epu16(a, b);
                    }

                    static inline __m128i add(
                            __m128i a,
                            __m128i b) {
                        return _mm_add_epi16(a, b);
                    }

                    static inline __m128i mullo(
                            __m128i a,
                            __m128i b) {
                        return _mm_mullo_epi16(a, b);
                    }

                    static inline uint16_t sum(
                            __m128i a) {
                        auto mm = _mm_add_epi16(a, _mm_srli_si128(a, 8));
                        mm = _mm_add_epi16(mm, _mm_srli_si128(mm, 4));
                        mm = _mm_add_epi16(mm, _mm_srli_si128(mm, 2));
                        return static_cast<uint16_t>(_mm_extract_epi16(mm, 0));
                    }

                    static inline void pack_right2(
                            uint16_t * & result,
                            __m128i a,
                            mask_t mask) {
                        if (mask) {
                            Private::pack_right2_uint16(result, a, mask);
                        }
                    }
                };

                template<>
                struct v2_mm128<uint32_t> {

                    typedef uint8_t mask_t;

                    static inline __m128i set1(
                            uint32_t value) {
                        return _mm_set1_epi32(value);
                    }

                    static inline __m128i set(
                            uint32_t v3,
                            uint32_t v2,
                            uint32_t v1,
                            uint32_t v0) {
                        return _mm_set_epi32(v3, v2, v1, v0);
                    }

                    static inline __m128i set_inc(
                            uint32_t v0) {
                        return _mm_set_epi32(v0 + 3, v0 + 2, v0 + 1, v0);
                    }

                    static inline __m128i set_inc(
                            uint32_t v0,
                            uint32_t inc) {
                        return _mm_set_epi32(v0 + 3 * inc, v0 + 2 * inc, v0 + inc, v0);
                    }

                    static inline __m128i min(
                            __m128i a,
                            __m128i b) {
                        return _mm_min_epu32(a, b);
                    }

                    static inline __m128i max(
                            __m128i a,
                            __m128i b) {
                        return _mm_max_epu32(a, b);
                    }

                    static inline __m128i add(
                            __m128i a,
                            __m128i b) {
                        return _mm_add_epi32(a, b);
                    }

                    static inline __m128i mullo(
                            __m128i a,
                            __m128i b) {
                        return _mm_mullo_epi32(a, b);
                    }

                    static inline __m128i geq(
                            __m128i a,
                            __m128i b) {
                        auto mm = max(a, b);
                        return _mm_cmpeq_epi32(a, mm);
                    }

                    static inline uint8_t geq_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<uint8_t>(_mm_movemask_ps(_mm_castsi128_ps(geq(a, b))));
                    }

                    static inline uint32_t sum(
                            __m128i a) {
                        auto mm = _mm_add_epi32(a, _mm_srli_si128(a, 8));
                        mm = _mm_add_epi32(mm, _mm_srli_si128(mm, 4));
                        return static_cast<uint32_t>(_mm_extract_epi32(mm, 0));
                    }

                    static inline void pack_right2(
                            uint32_t * & result,
                            __m128i a,
                            mask_t mask) {
                        if (mask) {
                            Private::pack_right2_uint32(result, a, mask);
                        }
                    }
                };

                template<>
                struct v2_mm128<uint64_t> {

                    typedef uint8_t mask_t;

                    static inline __m128i set1(
                            uint64_t value) {
                        return _mm_set1_epi64x(value);
                    }

                    static inline __m128i set(
                            uint64_t v1,
                            uint64_t v0) {
                        return _mm_set_epi64x(v1, v0);
                    }

                    static inline __m128i set_inc(
                            uint64_t v0) {
                        return _mm_set_epi64x(v0 + 1, v0);
                    }

                    static inline __m128i set_inc(
                            uint64_t v0,
                            uint64_t inc) {
                        return _mm_set_epi64x(v0 + inc, v0);
                    }

                    static inline __m128i min(
                            __m128i a,
                            __m128i b) {
                        return _mm_set_epi64x(std::min(static_cast<uint64_t>(_mm_extract_epi64(a, 1)), static_cast<uint64_t>(_mm_extract_epi64(b, 1))),
                                std::min(static_cast<uint64_t>(_mm_extract_epi64(a, 0)), static_cast<uint64_t>(_mm_extract_epi64(b, 0))));
                    }

                    static inline __m128i max(
                            __m128i a,
                            __m128i b) {
                        return _mm_set_epi64x(std::max(static_cast<uint64_t>(_mm_extract_epi64(a, 1)), static_cast<uint64_t>(_mm_extract_epi64(b, 1))),
                                std::max(static_cast<uint64_t>(_mm_extract_epi64(a, 0)), static_cast<uint64_t>(_mm_extract_epi64(b, 0))));
                    }

                    static inline __m128i add(
                            __m128i a,
                            __m128i b) {
                        return _mm_add_epi64(a, b);
                    }

                    static inline __m128i mullo(
                            __m128i a,
                            __m128i b) {
                        return _mm_set_epi64x(_mm_extract_epi64(a, 1) * _mm_extract_epi64(b, 1), _mm_extract_epi64(a, 0) * _mm_extract_epi64(b, 0));
                    }

                    static inline __m128i geq(
                            __m128i a,
                            __m128i b) {
                        auto mm = max(a, b);
                        return _mm_cmpeq_epi64(a, mm);
                    }

                    static inline uint8_t geq_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<uint8_t>(_mm_movemask_pd(_mm_castsi128_pd(geq(a, b))));
                    }

                    static inline uint64_t sum(
                            __m128i a) {
                        return static_cast<uint64_t>(_mm_extract_epi64(a, 0)) + static_cast<uint64_t>(_mm_extract_epi64(a, 1));
                    }

                    static inline void pack_right2(
                            uint64_t * & result,
                            __m128i a,
                            mask_t mask) {
                        if (mask) {
                            Private::pack_right2_uint64(result, a, mask);
                        }
                    }
                };

                namespace Private {
                    template<typename A, typename B, typename R>
                    struct V2_mm128_mul_add;

                    template<>
                    struct V2_mm128_mul_add<uint16_t, uint16_t, uint16_t> {

                        static inline __m128i doIt(
                                __m128i * a,
                                __m128i * b,
                                size_t & incA,
                                size_t & incB) {
                            incA = incB = 1;
                            return v2_mm128<uint16_t>::mullo(_mm_lddqu_si128(a), _mm_lddqu_si128(b));
                        }
                    };

                    template<>
                    struct V2_mm128_mul_add<uint16_t, uint16_t, uint32_t> {

                        static inline __m128i doIt(
                                __m128i * a,
                                __m128i * b,
                                size_t & incA,
                                size_t & incB) {
                            incA = incB = 1;
                            auto mm = _mm_mullo_epi32(_mm_cvtepi16_epi32(_mm_lddqu_si128(a)), _mm_cvtepi16_epi32(_mm_lddqu_si128(b)));
                            mm = _mm_add_epi32(mm, _mm_mullo_epi32(_mm_cvtepi16_epi32(_mm_srli_si128(_mm_lddqu_si128(a), 8)), _mm_cvtepi16_epi32(_mm_srli_si128(_mm_lddqu_si128(b), 8))));
                            return mm;
                        }
                    };

                    template<>
                    struct V2_mm128_mul_add<uint16_t, uint32_t, uint32_t> {

                        static inline __m128i doIt(
                                __m128i * a,
                                __m128i * b,
                                size_t & incA,
                                size_t & incB) {
                            incA = 1;
                            incB = 2;
                            auto mm = _mm_mullo_epi32(_mm_cvtepi16_epi32(_mm_lddqu_si128(a)), _mm_lddqu_si128(b + 1));
                            mm = _mm_add_epi32(mm, _mm_mullo_epi32(_mm_cvtepi16_epi32(_mm_srli_si128(_mm_lddqu_si128(a), 8)), _mm_lddqu_si128(b)));
                            return mm;
                        }
                    };

                    template<>
                    struct V2_mm128_mul_add<uint32_t, uint16_t, uint32_t> {

                        static inline __m128i doIt(
                                __m128i * a,
                                __m128i * b,
                                size_t & incA,
                                size_t & incB) {
                            return V2_mm128_mul_add<uint16_t, uint32_t, uint32_t>::doIt(b, a, incB, incA);
                        }
                    };

                    template<>
                    struct V2_mm128_mul_add<uint16_t, uint32_t, uint64_t> {

                        static inline __m128i doIt(
                                __m128i * a,
                                __m128i * b,
                                size_t & incA,
                                size_t & incB) {
                            incA = 1;
                            incB = 2;
                            auto mm = _mm_mul_epi32(_mm_cvtepi16_epi32(_mm_lddqu_si128(a)), _mm_lddqu_si128(b + 1));
                            mm = _mm_add_epi64(mm, _mm_mul_epi32(_mm_cvtepi16_epi32(_mm_srli_si128(_mm_lddqu_si128(a), 8)), _mm_lddqu_si128(b)));
                            return mm;
                        }
                    };

                    template<>
                    struct V2_mm128_mul_add<uint32_t, uint16_t, uint64_t> {

                        static inline __m128i doIt(
                                __m128i * a,
                                __m128i * b,
                                size_t & incA,
                                size_t & incB) {
                            return V2_mm128_mul_add<uint16_t, uint32_t, uint64_t>::doIt(b, a, incB, incA);
                        }
                    };

                    template<>
                    struct V2_mm128_mul_add<uint16_t, uint64_t, uint64_t> {

                        static inline __m128i doIt(
                                __m128i * a,
                                __m128i * b,
                                size_t & incA,
                                size_t & incB) {
                            incA = 1;
                            incB = 4;

                            auto mm = _mm_mullo_epi64(_mm_cvtepu16_epi64(_mm_lddqu_si128(a)), _mm_lddqu_si128(b));
                            mm = _mm_add_epi64(mm, _mm_mullo_epi64(_mm_cvtepu16_epi64(_mm_srli_si128(_mm_lddqu_si128(a), 4)), _mm_lddqu_si128(b + 1)));
                            mm = _mm_add_epi64(mm, _mm_mullo_epi64(_mm_cvtepu16_epi64(_mm_srli_si128(_mm_lddqu_si128(a), 8)), _mm_lddqu_si128(b + 2)));
                            mm = _mm_add_epi64(mm, _mm_mullo_epi64(_mm_cvtepu16_epi64(_mm_srli_si128(_mm_lddqu_si128(a), 12)), _mm_lddqu_si128(b + 3)));

                            return mm;
                        }
                    };

                    template<>
                    struct V2_mm128_mul_add<uint64_t, uint16_t, uint64_t> {

                        static inline __m128i doIt(
                                __m128i * a,
                                __m128i * b,
                                size_t & incA,
                                size_t & incB) {
                            return V2_mm128_mul_add<uint16_t, uint64_t, uint64_t>::doIt(b, a, incB, incA);
                        }
                    };

                    template<>
                    struct V2_mm128_mul_add<uint32_t, uint8_t, uint64_t> {

                        static inline __m128i doIt(
                                __m128i * a,
                                __m128i * b,
                                size_t & incA,
                                size_t & incB) {
                            incA = 4;
                            incB = 1;

                            auto pA = reinterpret_cast<uint32_t*>(a);
                            auto pB = reinterpret_cast<uint8_t*>(b);
                            uint64_t r1 = 0, r2 = 0;
                            for (size_t i = 0; i < 16; i += 2) {
                                r1 += static_cast<uint64_t>(pA[i]) * static_cast<uint64_t>(pB[i]);
                                r2 += static_cast<uint64_t>(pA[i + 1]) * static_cast<uint64_t>(pB[i + 1]);
                            }
                            __m128i mm = _mm_set_epi64x(r1, r2);

                            return mm;
                        }
                    };

                    template<>
                    struct V2_mm128_mul_add<uint32_t, uint32_t, uint32_t> {

                        static inline __m128i doIt(
                                __m128i * a,
                                __m128i * b,
                                size_t & incA,
                                size_t & incB) {
                            incA = incB = 1;
                            return v2_mm128<uint32_t>::mullo(_mm_lddqu_si128(a), _mm_lddqu_si128(b));
                        }
                    };

                    template<>
                    struct V2_mm128_mul_add<uint32_t, uint32_t, uint64_t> {

                        static inline __m128i doIt(
                                __m128i * a,
                                __m128i * b,
                                size_t & incA,
                                size_t & incB) {
                            incA = incB = 1;
                            auto mm = _mm_mul_epu32(*a, *b);
                            return _mm_add_epi32(mm, _mm_mul_epu32(_mm_srli_si128(_mm_lddqu_si128(a), 8), _mm_srli_si128(_mm_lddqu_si128(b), 8)));
                        }
                    };

                    template<>
                    struct V2_mm128_mul_add<uint64_t, uint64_t, uint64_t> {

                        static inline __m128i doIt(
                                __m128i * a,
                                __m128i * b,
                                size_t & incA,
                                size_t & incB) {
                            incA = incB = 1;
                            return v2_mm128<uint64_t>::mullo(_mm_lddqu_si128(a), _mm_lddqu_si128(b));
                        }
                    };
                }

                template<typename A, typename B, typename R>
                inline __m128i v2_mm128_mul_add(
                        __m128i * a,
                        __m128i * b,
                        size_t & incA,
                        size_t & incB) {
                    return Private::V2_mm128_mul_add<A, B, R>::doIt(a, b, incA, incB);
                }

                namespace Private {
                    template<typename TA, size_t firstA, typename TB, size_t firstB, typename R>
                    struct V2_mm128_mullo;

                    template<size_t firstA, size_t firstB>
                    struct V2_mm128_mullo<uint16_t, firstA, uint64_t, firstB, uint64_t> {

                        static inline __m128i doIt(
                                __m128i & a,
                                __m128i & b) {
                            auto r0 = static_cast<uint64_t>(_mm_extract_epi16(a, firstA)) * _mm_extract_epi64(b, firstB);
                            auto r1 = static_cast<uint64_t>(_mm_extract_epi16(a, firstA + 1)) * _mm_extract_epi64(b, firstB + 1);
                            return _mm_set_epi64x(r1, r0);
                        }
                    };

                    template<size_t firstA, size_t firstB>
                    struct V2_mm128_mullo<uint64_t, firstA, uint16_t, firstB, uint64_t> {

                        static inline __m128i doIt(
                                __m128i & a,
                                __m128i & b) {
                            return V2_mm128_mullo<uint16_t, firstB, uint64_t, firstA, uint64_t>::doIt(b, a);
                        }
                    };
                }

                template<typename TA, size_t firstA, typename TB, size_t firstB, typename R>
                inline __m128i v2_mm128_mullo(
                        __m128i & a,
                        __m128i & b) {
                    return Private::V2_mm128_mullo<TA, firstA, TB, firstB, R>::doIt(a, b);
                }

                namespace Private {
                    template<typename T, typename R>
                    struct V2_mm128_cvt;

                    template<>
                    struct V2_mm128_cvt<uint16_t, uint8_t> {

                        static inline __m128i doIt(
                                __m128i & mm) {
                            return _mm_shuffle_epi8(mm, _mm_set_epi64x(0xFFFFFFFFFFFFFFFF, 0x0E0C0A0806040200));
                        }
                    };

                    template<>
                    struct V2_mm128_cvt<uint16_t, uint16_t> {

                        static inline __m128i doIt(
                                __m128i & mm) {
                            return mm;
                        }
                    };

                    template<>
                    struct V2_mm128_cvt<uint32_t, uint16_t> {

                        static inline __m128i doIt(
                                __m128i & mm) {
                            return _mm_shuffle_epi8(mm, _mm_set_epi64x(0xFFFFFFFFFFFFFFFF, 0x0D0C090805040100));
                        }
                    };

                    template<>
                    struct V2_mm128_cvt<uint32_t, uint32_t> {

                        static inline __m128i doIt(
                                __m128i & mm) {
                            return mm;
                        }
                    };

                    template<>
                    struct V2_mm128_cvt<uint64_t, uint32_t> {

                        static inline __m128i doIt(
                                __m128i & mm) {
                            return _mm_shuffle_epi8(mm, _mm_set_epi64x(0xFFFFFFFFFFFFFFFF, 0x0B0A090803020100));
                        }
                    };

                    template<>
                    struct V2_mm128_cvt<uint64_t, uint64_t> {

                        static inline __m128i doIt(
                                __m128i & mm) {
                            return mm;
                        }
                    };
                }

                template<typename T, typename R>
                inline __m128i v2_mm128_cvt(
                        __m128i & mm) {
                    return Private::V2_mm128_cvt<T, R>::doIt(mm);
                }

            }
        }
    }
}

#endif /* SSE_HPP */
