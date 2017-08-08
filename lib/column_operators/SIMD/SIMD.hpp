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
 * File:   SIMD.hpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 09-08-2017 00:18
 */
#ifndef LIB_COLUMN_OPERATORS_SIMD_SIMD_HPP_
#define LIB_COLUMN_OPERATORS_SIMD_SIMD_HPP_

#include "SSE.hpp"
#ifdef __AVX2__
#include "AVX2.hpp"
#endif

namespace ahead {
    namespace bat {
        namespace ops {
            namespace simd {
                template<typename V, typename T>
                struct v2_mm;

                template<typename T>
                struct v2_mm<__m128i, T> {
                    typedef ahead::bat::ops::sse::v2_mm128<T> BASE;
                    typedef typename BASE::mask_t mask_t;

                    static inline __m128i set1(
                            T value) {
                        return BASE::set1(value);
                    }

                    static inline __m128i set(
                            T v15,
                            T v14,
                            T v13,
                            T v12,
                            T v11,
                            T v10,
                            T v9,
                            T v8,
                            T v7,
                            T v6,
                            T v5,
                            T v4,
                            T v3,
                            T v2,
                            T v1,
                            T v0) {
                        return BASE::set(v15, v14, v13, v12, v11, v10, v9, v8, v7, v6, v5, v4, v3, v2, v1, v0);
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
                };

#ifdef __AVX2__
                template<typename T>
                struct v2_mm<__m256i, T> {
                    typedef ahead::bat::ops::avx2::v2_mm256<T> BASE;
                    typedef typename BASE::mask_t mask_t;

                    static inline __m256i set1(
                            T value) {
                        return BASE::set1(value);
                    }

                    static inline __m256i set(
                            T v15,
                            T v14,
                            T v13,
                            T v12,
                            T v11,
                            T v10,
                            T v9,
                            T v8,
                            T v7,
                            T v6,
                            T v5,
                            T v4,
                            T v3,
                            T v2,
                            T v1,
                            T v0) {
                        return BASE::set(v15, v14, v13, v12, v11, v10, v9, v8, v7, v6, v5, v4, v3, v2, v1, v0);
                    }

                    static inline __m256i set_inc(
                            T v0) {
                        return BASE::set_inc(v0);
                    }

                    static inline __m256i set_inc(
                            T v0,
                            uint16_t inc) {
                        return BASE::set_inc(v0, inc);
                    }

                    static inline __m256i min(
                            __m256i a,
                            __m256i b) {
                        return BASE::min(a, b);
                    }

                    static inline __m256i max(
                            __m256i a,
                            __m256i b) {
                        return BASE::max(a, b);
                    }

                    static inline __m256i mullo(
                            __m256i a,
                            __m256i b) {
                        return BASE::mullo(a, b);
                    }

                    static inline T sum(
                            __m256i a) {
                        return BASE::sum(a);
                    }

                    static inline __m256i pack_right(
                            __m256i a,
                            mask_t mask) {
                        return BASE::pack_right(a, mask);
                    }

                    static inline void pack_right2(
                            T * & result,
                            __m256i a,
                            mask_t mask) {
                        BASE::pack_right2(result, a, mask);
                    }
                };
#endif

            }
        }
    }
}

#endif /* LIB_COLUMN_OPERATORS_SIMD_SIMD_HPP_ */
