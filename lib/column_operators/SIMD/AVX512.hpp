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
 * File:   AVX512.hpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 09. August 2017, 14:19
 */

#ifndef LIB_COLUMN_OPERATORS_SIMD_AVX512_HPP_
#define LIB_COLUMN_OPERATORS_SIMD_AVX512_HPP_

#if defined(AHEAD_AVX512) or ( defined(__AVX512CD__) and defined(__AVX512ER__) and defined(__AVX512F__) and defined(__AVX512PF__) )
#ifndef AHEAD_AVX512
#define AHEAD_AVX512
#endif /* AHEAD_AVX512 */

#include <algorithm>
#include <cstdint>
#include <immintrin.h>

#include "SIMD.hpp"

namespace ahead {
    namespace bat {
        namespace ops {
            namespace simd {
                namespace avx512 {

                    template<typename T>
                    struct mm512;

                    namespace Private {
                        template<size_t current = 0>
                        inline void pack_right2_uint8(
                                uint8_t * & result,
                                __m512i & a,
                                uint64_t mask) {
                            *result = reinterpret_cast<uint8_t*>(&a)[current];
                            result += (mask >> current) & 0x1;
                            pack_right2_uint8<current + 1>(result, a, mask);
                        }

                        template<>
                        inline void pack_right2_uint8<63>(
                                uint8_t * & result,
                                __m512i & a,
                                uint64_t mask) {
                            *result = reinterpret_cast<uint8_t*>(&a)[63];
                            result += (mask >> 31) & 0x1;
                        }

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
                    struct mm512<uint8_t> {

                        typedef uint64_t mask_t;

                        static inline __m512i set1(
                                uint8_t value) {
                            return _mm512_set1_epi8(value);
                        }

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
                            return _mm512_set_epi8(v63, v62, v61, v60, v59, v58, v57, v56, v55, v54, v53, v52, v51, v50, v49, v48, v47, v46, v45, v44, v43, v42, v41, v40, v39, v38, v37, v36, v35, v34,
                                    v33, v32, v31, v30, v29, v28, v27, v26, v25, v24, v23, v22, v21, v20, v19, v18, v17, v16, v15, v14, v13, v12, v11, v10, v9, v8, v7, v6, v5, v4, v3, v2, v1, v0);
                        }

                        static inline __m512i set_inc(
                                uint8_t v0) {
                            return _mm512_set_epi8(v0 + 63, v0 + 62, v0 + 61, v0 + 60, v0 + 59, v0 + 58, v0 + 57, v0 + 56, v0 + 55, v0 + 54, v0 + 53, v0 + 52, v0 + 51, v0 + 50, v0 + 49, v0 + 48,
                                    v0 + 47, v0 + 46, v0 + 45, v0 + 44, v0 + 43, v0 + 42, v0 + 41, v0 + 40, v0 + 39, v0 + 38, v0 + 37, v0 + 36, v0 + 35, v0 + 34, v0 + 33, v0 + 32, v0 + 31, v0 + 30,
                                    v0 + 29, v0 + 28, v0 + 27, v0 + 26, v0 + 25, v0 + 24, v0 + 23, v0 + 22, v0 + 21, v0 + 20, v0 + 19, v0 + 18, v0 + 17, v0 + 16, v0 + 15, v0 + 14, v0 + 13, v0 + 12,
                                    v0 + 11, v0 + 10, v0 + 9, v0 + 8, v0 + 7, v0 + 6, v0 + 5, v0 + 4, v0 + 3, v0 + 2, v0 + 1, v0);
                        }

                        static inline __m512i set_inc(
                                uint8_t v0,
                                uint16_t inc) {
                            return _mm512_set_epi8(v0 + 63 * inc, v0 + 62 * inc, v0 + 61 * inc, v0 + 60 * inc, v0 + 59 * inc, v0 + 58 * inc, v0 + 57 * inc, v0 + 56 * inc, v0 + 55 * inc, v0 + 54 * inc,
                                    v0 + 53 * inc, v0 + 52 * inc, v0 + 51 * inc, v0 + 50 * inc, v0 + 49 * inc, v0 + 48 * inc, v0 + 47 * inc, v0 + 46 * inc, v0 + 45 * inc, v0 + 44 * inc, v0 + 43 * inc,
                                    v0 + 42 * inc, v0 + 41 * inc, v0 + 40 * inc, v0 + 39 * inc, v0 + 38 * inc, v0 + 37 * inc, v0 + 36 * inc, v0 + 35 * inc, v0 + 34 * inc, v0 + 33 * inc, v0 + 32 * inc,
                                    v0 + 31 * inc, v0 + 30 * inc, v0 + 29 * inc, v0 + 28 * inc, v0 + 27 * inc, v0 + 26 * inc, v0 + 25 * inc, v0 + 24 * inc, v0 + 23 * inc, v0 + 22 * inc, v0 + 21 * inc,
                                    v0 + 20 * inc, v0 + 19 * inc, v0 + 18 * inc, v0 + 17 * inc, v0 + 16 * inc, v0 + 15 * inc, v0 + 14 * inc, v0 + 13 * inc, v0 + 12 * inc, v0 + 11 * inc, v0 + 10 * inc,
                                    v0 + 9 * inc, v0 + 8 * inc, v0 + 7 * inc, v0 + 6 * inc, v0 + 5 * inc, v0 + 4 * inc, v0 + 3 * inc, v0 + 2 * inc, v0 + inc, v0);
                        }

                        static inline __m512i min(
                                __m512i a,
                                __m512i b) {
                            return _mm512_min_epu8(a, b);
                        }

                        static inline __m512i max(
                                __m512i a,
                                __m512i b) {
                            return _mm512_max_epu8(a, b);
                        }

                        static inline __m512i geq(
                                __m512i a,
                                __m512i b) {
#ifdef __AVX512BW__
                            auto mm = max(a, b);
                            return _mm512_cmpeq_epi8(a, mm);
#else
                            throw std::runtime_error("__m512i geq uint8_t not supported yet");
#endif
                        }

                        static inline uint64_t geq_mask(
                                __m512i a,
                                __m512i b) {
#ifdef __AVX512BW__
                            return _mm512_cmpge_epi8_mask(a, b);
#else
                            throw std::runtime_error("__m512i geq_mask uint8_t not supported yet");
#endif
                        }

                        static inline __m512i mullo(
                                __m512i a,
                                __m512i b) {
                            throw std::runtime_error("__m512i mullo uint8_t not supported yet");
                        }

                        static inline uint8_t sum(
                                __m512i a) {
                            throw std::runtime_error("__m512i sum uint8_t not supported yet");
                        }

                        static inline __m512i pack_right(
                                __m512i a,
                                mask_t mask) {
                            throw std::runtime_error("__m512i pack_right uint8_t not supported yet");
                        }

                        static inline void pack_right2(
                                uint8_t * & result,
                                __m512i a,
                                mask_t mask) {
                            if (mask) {
                                Private::pack_right2_uint8(result, a, mask);
                            }
                        }
                    };

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
                            return _mm512_set_epi16(v31, v30, v29, v28, v27, v26, v25, v24, v23, v22, v21, v20, v19, v18, v17, v16, v15, v14, v13, v12, v11, v10, v9, v8, v7, v6, v5, v4, v3, v2, v1,
                                    v0);
                        }

                        static inline __m512i set_inc(
                                uint16_t v0) {
                            return _mm512_set_epi16(v0 + 31, v0 + 30, v0 + 29, v0 + 28, v0 + 27, v0 + 26, v0 + 25, v0 + 24, v0 + 23, v0 + 22, v0 + 21, v0 + 20, v0 + 19, v0 + 18, v0 + 17, v0 + 16,
                                    v0 + 15, v0 + 14, v0 + 13, v0 + 12, v0 + 11, v0 + 10, v0 + 9, v0 + 8, v0 + 7, v0 + 6, v0 + 5, v0 + 4, v0 + 3, v0 + 2, v0 + 1, v0);
                        }

                        static inline __m512i set_inc(
                                uint16_t v0,
                                uint16_t inc) {
                            return _mm512_set_epi16(v0 + 31 * inc, v0 + 30 * inc, v0 + 29 * inc, v0 + 28 * inc, v0 + 27 * inc, v0 + 26 * inc, v0 + 25 * inc, v0 + 24 * inc, v0 + 23 * inc,
                                    v0 + 22 * inc, v0 + 21 * inc, v0 + 20 * inc, v0 + 19 * inc, v0 + 18 * inc, v0 + 17 * inc, v0 + 16 * inc, v0 + 15 * inc, v0 + 14 * inc, v0 + 13 * inc, v0 + 12 * inc,
                                    v0 + 11 * inc, v0 + 10 * inc, v0 + 9 * inc, v0 + 8 * inc, v0 + 7 * inc, v0 + 6 * inc, v0 + 5 * inc, v0 + 4 * inc, v0 + 3 * inc, v0 + 2 * inc, v0 + inc, v0);
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
                            if (mask) {
                                Private::pack_right2_uint16(result, a, mask);
                            }
                        }
                    };

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
                            auto mask = _mm512_cmpge_epi32_mask(a, b);
                            return _mm512_set_epi32(MASK(0x8000), MASK(0x4000), MASK(0x2000), MASK(0x1000), MASK(0x0800), MASK(0x0400), MASK(0x0200), MASK(0x0100), MASK(0x0080), MASK(0x0040),
                                    MASK(0x0020), MASK(0x0010), MASK(0x0008), MASK(0x0004), MASK(0x0002), MASK(0x0001));
#undef MASK
                        }

                        static inline uint16_t geq_mask(
                                __m512i a,
                                __m512i b) {
                            return _mm512_cmpge_epi32_mask(a, b);
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
                            if (mask) {
                                Private::pack_right2_uint32(result, a, mask);
                            }
                        }

                    private:
                        static const __m512i * const SHUFFLE_TABLE;
                    };

                    template<>
                    struct mm512<uint64_t> {

                        typedef uint8_t mask_t;

                        static inline __m512i set1(
                                uint64_t value) {
                            return _mm512_set1_epi64x(value);
                        }

                        static inline __m512i set(
                                uint64_t v3,
                                uint64_t v2,
                                uint64_t v1,
                                uint64_t v0) {
                            return _mm512_set_epi64x(v3, v2, v1, v0);
                        }

                        static inline __m512i set_inc(
                                uint64_t v0) {
                            return _mm512_set_epi64x(v0 + 3, v0 + 2, v0 + 1, v0);
                        }

                        static inline __m512i set_inc(
                                uint64_t v0,
                                uint64_t inc) {
                            return _mm512_set_epi64x(v0 + 3 * inc, v0 + 2 * inc, v0 + inc, v0);
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
                            auto mm = max(a, b);
                            return _mm512_cmpeq_epi64(a, mm);
                        }

                        static inline uint8_t geq_mask(
                                __m512i a,
                                __m512i b) {
                            return static_cast<uint8_t>(_mm512_movemask_pd(_mm512_castsi256_pd(geq(a, b))));
                        }

                        static inline uint64_t sum(
                                __m512i a) {
                            auto mm = _mm512_add_epi64(a, _mm512_srli_si256(a, 8));
                            return static_cast<uint64_t>(_mm512_extract_epi64(a, 0)) + static_cast<uint64_t>(_mm512_extract_epi64(a, 2));
                        }

                        static inline __m512i pack_right(
                                __m512i a,
                                mask_t mask) {
                            return Private::_mm512_shuffle256_epi8(a, SHUFFLE_TABLE[mask]);
                        }

                        static inline void pack_right2(
                                uint64_t * & result,
                                __m512i a,
                                mask_t mask) {
                            if (mask) {
                                Private::pack_right2_uint64(result, a, mask);
                            }
                        }

                    private:
                        static const __m512i * const SHUFFLE_TABLE;
                    };

                }

                template<typename T>
                struct mm<__m512i, T> : public avx512::mm512<__m512i, T> {
                    typedef ahead::bat::ops::avx512::mm512<T> BASE;
                    typedef typename BASE::mask_t mask_t;

                    using avx512::mm512<__m512i, T>::set;
                    using avx512::mm512<__m512i, T>::set1;
                    using avx512::mm512<__m512i, T>::set_inc;
                    using avx512::mm512<__m512i, T>::min;
                    using avx512::mm512<__m512i, T>::max;
                    using avx512::mm512<__m512i, T>::add;

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

            }
        }
    }
}

#endif /* AHEAD_AVX512 */

#endif /* LIB_COLUMN_OPERATORS_SIMD_AVX512_HPP_ */
