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

#include "SIMDBase.hpp"

#include "SSE.hpp"
#ifdef __AVX2__
#include "AVX2.hpp"
#endif

// #ifdef AHEAD_AVX512
// #include "AVX512.hpp"
// #elif defined(__AVX512CD__) and defined(__AVX512ER__) and defined(__AVX512F__) and defined(__AVX512PF__)
// #define AHEAD_AVX512
// #include "AVX512.hpp"
// #endif

namespace ahead {
    namespace bat {
        namespace ops {
            namespace simd {

#ifdef AHEAD_AVX512
            template<typename T>
            struct v2_mm<__m512i, T> {
                typedef ahead::bat::ops::avx512::v2_mm512<T> BASE;
                typedef typename BASE::mask_t mask_t;

                static inline __m512i set1(
                        T value) {
                    return BASE::set1(value);
                }

                static inline __m512i set_inc(
                        T v0) {
                    return BASE::set_inc(v0);
                }

                static inline __m512i set_inc(
                        T v0,
                        uint16_t inc) {
                    return BASE::set_inc(v0, inc);
                }

                static inline __m512i min(
                        __m512i a,
                        __m512i b) {
                    return BASE::min(a, b);
                }

                static inline __m512i max(
                        __m512i a,
                        __m512i b) {
                    return BASE::max(a, b);
                }

                static inline __m512i add(
                        __m512i a,
                        __m512i b) {
                    return BASE::add(a, b);
                }

                static inline __m512i mullo(
                        __m512i a,
                        __m512i b) {
                    return BASE::mullo(a, b);
                }

                static inline T sum(
                        __m512i a) {
                    return BASE::sum(a);
                }

                static inline __m512i pack_right(
                        __m512i a,
                        mask_t mask) {
                    return BASE::pack_right(a, mask);
                }

                static inline void pack_right2(
                        T * & result,
                        __m512i a,
                        mask_t mask) {
                    BASE::pack_right2(result, a, mask);
                }

                static inline __m512i loadu(
                        __m512i * src) {
                    return _mm512_lddqu_si512(src);
                }

                static inline void storeu(
                        void * dst,
                        __m512i src) {
                    _mm512_storeu_si512(dst, src);
                }
            };
#endif

#ifdef __AVX2__
            template<typename T>
            struct v2_mm<__m256i, T> {
                typedef ahead::bat::ops::avx2::v2_mm256<T> BASE;
                typedef typename BASE::mask_t mask_t;

                static inline __m256i set1(
                        T value) {
                    return BASE::set1(value);
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

                static inline __m256i add(
                        __m256i a,
                        __m256i b) {
                    return BASE::add(a, b);
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

                static inline __m256i loadu(
                        __m256i * src) {
                    return _mm256_lddqu_si256(src);
                }

                static inline void storeu(
                        __m256i * dst,
                        __m256i src) {
                    _mm256_storeu_si256(dst, src);
                }
            };
#endif

#ifdef AHEAD_AVX512
            template<>
            struct v2_mmx<__m512i, uint8_t> {
                static inline __m512i set(
                        uint8_t v63,
                        uint8_t v62,
                        uint8_t v61,
                        uint8_t v60,
                        uint8_t v59,
                        uint8_t v58,
                        uint8_t v57,
                        uint8_t v56,
                        uint8_t v55,
                        uint8_t v54,
                        uint8_t v53,
                        uint8_t v52,
                        uint8_t v51,
                        uint8_t v50,
                        uint8_t v49,
                        uint8_t v48,
                        uint8_t v47,
                        uint8_t v46,
                        uint8_t v45,
                        uint8_t v44,
                        uint8_t v43,
                        uint8_t v42,
                        uint8_t v41,
                        uint8_t v40,
                        uint8_t v39,
                        uint8_t v38,
                        uint8_t v37,
                        uint8_t v36,
                        uint8_t v35,
                        uint8_t v34,
                        uint8_t v33,
                        uint8_t v32,
                        uint8_t v31,
                        uint8_t v30,
                        uint8_t v29,
                        uint8_t v28,
                        uint8_t v27,
                        uint8_t v26,
                        uint8_t v25,
                        uint8_t v24,
                        uint8_t v23,
                        uint8_t v22,
                        uint8_t v21,
                        uint8_t v20,
                        uint8_t v19,
                        uint8_t v18,
                        uint8_t v17,
                        uint8_t v16,
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
                    return ahead::bat::ops::avx512::v2_mm512<uint8_t>::set(v63, v62, v61, v60, v59, v58, v57, v56, v55, v54, v53, v52, v51, v50, v49, v48, v47, v46, v45, v44, v43, v42, v41, v40, v39, v38, v37, v36, v35, v34, v33, v32, v31, v30, v29, v28, v27, v26, v25, v24, v23, v22, v21, v20, v19, v18, v17, v16, v15, v14, v13, v12, v11, v10, v9, v8, v7, v6, v5, v4, v3, v2, v1, v0);
                }
            };

            template<>
            struct v2_mmx<__m512i, uint16_t> {
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
                    return _mm512_set_epi16(v31, v30, v29, v28, v27, v26, v25, v24, v23, v22, v21, v20, v19, v18, v17, v16, v15, v14, v13, v12, v11, v10, v9, v8, v7, v6, v5, v4, v3, v2, v1, v0);
                }
            };

            template<>
            struct v2_mmx<__m512i, uint32_t> {
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
            };

            template<>
            struct v2_mmx<__m512i, uint64_t> {
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
            };
#endif

#ifdef __AVX2__
            template<>
            struct v2_mmx<__m256i, uint8_t> {
                static inline __m256i set(
                        uint8_t v31,
                        uint8_t v30,
                        uint8_t v29,
                        uint8_t v28,
                        uint8_t v27,
                        uint8_t v26,
                        uint8_t v25,
                        uint8_t v24,
                        uint8_t v23,
                        uint8_t v22,
                        uint8_t v21,
                        uint8_t v20,
                        uint8_t v19,
                        uint8_t v18,
                        uint8_t v17,
                        uint8_t v16,
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
                    return _mm256_set_epi8(v31, v30, v29, v28, v27, v26, v25, v24, v23, v22, v21, v20, v19, v18, v17, v16, v15, v14, v13, v12, v11, v10, v9, v8, v7, v6, v5, v4, v3, v2, v1, v0);
                }
            };

            template<>
            struct v2_mmx<__m256i, uint16_t> {
                static inline __m256i set(
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
                    return _mm256_set_epi16(v15, v14, v13, v12, v11, v10, v9, v8, v7, v6, v5, v4, v3, v2, v1, v0);
                }
            };

            template<>
            struct v2_mmx<__m256i, uint32_t> {
                static inline __m256i set(
                        uint32_t v7,
                        uint32_t v6,
                        uint32_t v5,
                        uint32_t v4,
                        uint32_t v3,
                        uint32_t v2,
                        uint32_t v1,
                        uint32_t v0) {
                    return _mm256_set_epi32(v7, v6, v5, v4, v3, v2, v1, v0);
                }
            };

            template<>
            struct v2_mmx<__m256i, uint64_t> {
                static inline __m256i set(
                        uint64_t v3,
                        uint64_t v2,
                        uint64_t v1,
                        uint64_t v0) {
                    return _mm256_set_epi64x(v3, v2, v1, v0);
                }
            };
#endif

        }
    }
}
}

#endif /* LIB_COLUMN_OPERATORS_SIMD_SIMD_HPP_ */
