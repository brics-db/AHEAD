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

#pragma once

#define LIB_COLUMN_OPERATORS_SIMD_SSE_HPP_

#include <algorithm>
#include <cstdint>
#include <immintrin.h>

#include <column_operators/functors.hpp>

#include "SIMDBase.hpp"

namespace ahead {
    namespace bat {
        namespace ops {
            namespace simd {
                namespace sse {

                    template<typename ...Types>
                    struct v2_mm128;

                    namespace Private {
                        template<size_t current = 0>
                        inline void pack_right2_uint8(
                                uint8_t * & result,
                                __m128i & a,
                                uint16_t mask) {
                            *result = reinterpret_cast<uint8_t*>(&a)[current];
                            result += (mask >> current) & 0x1;
                            pack_right2_uint8<current + 1>(result, a, mask);
                        }

                        template<>
                        inline void pack_right2_uint8<15>(
                                uint8_t * & result,
                                __m128i & a,
                                uint16_t mask) {
                            *result = reinterpret_cast<uint8_t*>(&a)[15];
                            result += (mask >> 15) & 0x1;
                        }

                        template<size_t current = 0>
                        inline void pack_right2_uint16(
                                uint16_t * & result,
                                __m128i & a,
                                uint8_t mask) {
                            *result = reinterpret_cast<uint16_t*>(&a)[current];
                            result += (mask >> current) & 0x1;
                            pack_right2_uint16<current + 1>(result, a, mask);
                        }

                        template<>
                        inline void pack_right2_uint16<7>(
                                uint16_t * & result,
                                __m128i & a,
                                uint8_t mask) {
                            *result = reinterpret_cast<uint16_t*>(&a)[7];
                            result += (mask >> 7) & 0x1;
                        }

                        template<size_t current = 0>
                        inline void pack_right2_uint32(
                                uint32_t * & result,
                                __m128i & a,
                                uint8_t mask) {
                            *result = reinterpret_cast<uint32_t*>(&a)[current];
                            result += (mask >> current) & 0x1;
                            pack_right2_uint32<current + 1>(result, a, mask);
                        }

                        template<>
                        inline void pack_right2_uint32<3>(
                                uint32_t * & result,
                                __m128i & a,
                                uint8_t mask) {
                            *result = reinterpret_cast<uint32_t*>(&a)[3];
                            result += (mask >> 3) & 0x1;
                        }

                        template<size_t current = 0>
                        inline void pack_right2_uint64(
                                uint64_t * & result,
                                __m128i & a,
                                uint8_t mask) {
                            *result = reinterpret_cast<uint64_t*>(&a)[current];
                            result += (mask >> current) & 0x1;
                            pack_right2_uint64<current + 1>(result, a, mask);
                        }

                        template<>
                        inline void pack_right2_uint64<1>(
                                uint64_t * & result,
                                __m128i & a,
                                uint8_t mask) {
                            *result = reinterpret_cast<uint64_t*>(&a)[1];
                            result += (mask >> 1) & 0x1;
                        }
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

                }
            }
        }
    }
}

#include "SSE_uint08.tcc"
#include "SSE_uint16.tcc"
#include "SSE_uint32.tcc"
#include "SSE_uint64.tcc"

namespace ahead {
    namespace bat {
        namespace ops {
            namespace simd {

                template<>
                struct v2_mmx<__m128i, uint8_t> {
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
                };

                template<>
                struct v2_mmx<__m128i, uint16_t> {
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
                };

                template<>
                struct v2_mmx<__m128i, uint32_t> {
                    static inline __m128i set(
                            uint32_t v3,
                            uint32_t v2,
                            uint32_t v1,
                            uint32_t v0) {
                        return _mm_set_epi32(v3, v2, v1, v0);
                    }
                };

                template<>
                struct v2_mmx<__m128i, uint64_t> {
                    static inline __m128i set(
                            uint64_t v1,
                            uint64_t v0) {
                        return _mm_set_epi64x(v1, v0);
                    }
                };

                template<typename T>
                struct v2_mm<__m128i, T> {
                    typedef ahead::bat::ops::simd::sse::v2_mm128<T> BASE;
                    typedef typename BASE::mask_t mask_t;

                    using v2_mmx<__m128i, T>::set;

                    static inline __m128i set1(
                            T value) {
                        return BASE::set1(value);
                    }

                    static inline __m128i set_inc(
                            T v0) {
                        return BASE::set_inc(v0);
                    }

                    static inline __m128i set_inc(
                            T v0,
                            uint16_t inc) {
                        return BASE::set_inc(v0, inc);
                    }

                    static inline __m128i min(
                            __m128i a,
                            __m128i b) {
                        return BASE::min(a, b);
                    }

                    static inline __m128i max(
                            __m128i a,
                            __m128i b) {
                        return BASE::max(a, b);
                    }

                    static inline __m128i add(
                            __m128i a,
                            __m128i b) {
                        return BASE::add(a, b);
                    }

                    static inline __m128i mullo(
                            __m128i a,
                            __m128i b) {
                        return BASE::mullo(a, b);
                    }

                    static inline T sum(
                            __m128i a) {
                        return BASE::sum(a);
                    }

                    static inline __m128i pack_right(
                            __m128i a,
                            mask_t mask) {
                        return BASE::pack_right(a, mask);
                    }

                    static inline void pack_right2(
                            T * & result,
                            __m128i a,
                            mask_t mask) {
                        BASE::pack_right2(result, a, mask);
                    }

                    static inline __m128i loadu(
                            __m128i * src) {
                        return _mm_lddqu_si128(src);
                    }

                    static inline void storeu(
                            __m128i * dst,
                            __m128i src) {
                        _mm_storeu_si128(dst, src);
                    }
                };

            }
        }
    }
}
