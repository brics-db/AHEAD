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
 * AVX512_uint08.tcc
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
#ifdef __AVX512BW__
                            return _mm512_set_epi8(v63, v62, v61, v60, v59, v58, v57, v56, v55, v54, v53, v52, v51, v50, v49, v48, v47, v46, v45, v44, v43, v42, v41, v40, v39, v38, v37, v36, v35, v34,
                                    v33, v32, v31, v30, v29, v28, v27, v26, v25, v24, v23, v22, v21, v20, v19, v18, v17, v16, v15, v14, v13, v12, v11, v10, v9, v8, v7, v6, v5, v4, v3, v2, v1, v0);
#else
                            return _mm512_set_epi32((static_cast<uint32_t>(v63) << 24) | (static_cast<uint32_t>(v62) << 16) | (static_cast<uint32_t>(v61) << 8) | (static_cast<uint32_t>(v60)),
                                    (static_cast<uint32_t>(v59) << 24) | (static_cast<uint32_t>(v58) << 16) | (static_cast<uint32_t>(v57) << 8) | (static_cast<uint32_t>(v56)),
                                    (static_cast<uint32_t>(v55) << 24) | (static_cast<uint32_t>(v54) << 16) | (static_cast<uint32_t>(v53) << 8) | (static_cast<uint32_t>(v52)),
                                    (static_cast<uint32_t>(v51) << 24) | (static_cast<uint32_t>(v50) << 16) | (static_cast<uint32_t>(v49) << 8) | (static_cast<uint32_t>(v48)),
                                    (static_cast<uint32_t>(v47) << 24) | (static_cast<uint32_t>(v46) << 16) | (static_cast<uint32_t>(v45) << 8) | (static_cast<uint32_t>(v44)),
                                    (static_cast<uint32_t>(v43) << 24) | (static_cast<uint32_t>(v42) << 16) | (static_cast<uint32_t>(v41) << 8) | (static_cast<uint32_t>(v40)),
                                    (static_cast<uint32_t>(v39) << 24) | (static_cast<uint32_t>(v38) << 16) | (static_cast<uint32_t>(v37) << 8) | (static_cast<uint32_t>(v36)),
                                    (static_cast<uint32_t>(v35) << 24) | (static_cast<uint32_t>(v34) << 16) | (static_cast<uint32_t>(v33) << 8) | (static_cast<uint32_t>(v32)),
                                    (static_cast<uint32_t>(v31) << 24) | (static_cast<uint32_t>(v30) << 16) | (static_cast<uint32_t>(v29) << 8) | (static_cast<uint32_t>(v28)),
                                    (static_cast<uint32_t>(v27) << 24) | (static_cast<uint32_t>(v26) << 16) | (static_cast<uint32_t>(v25) << 8) | (static_cast<uint32_t>(v24)),
                                    (static_cast<uint32_t>(v23) << 24) | (static_cast<uint32_t>(v22) << 16) | (static_cast<uint32_t>(v21) << 8) | (static_cast<uint32_t>(v20)),
                                    (static_cast<uint32_t>(v19) << 24) | (static_cast<uint32_t>(v18) << 16) | (static_cast<uint32_t>(v17) << 8) | (static_cast<uint32_t>(v16)),
                                    (static_cast<uint32_t>(v15) << 24) | (static_cast<uint32_t>(v14) << 16) | (static_cast<uint32_t>(v13) << 8) | (static_cast<uint32_t>(v12)),
                                    (static_cast<uint32_t>(v11) << 24) | (static_cast<uint32_t>(v10) << 16) | (static_cast<uint32_t>(v9) << 8) | (static_cast<uint32_t>(v8)),
                                    (static_cast<uint32_t>(v7) << 24) | (static_cast<uint32_t>(v6) << 16) | (static_cast<uint32_t>(v5) << 8) | (static_cast<uint32_t>(v4)),
                                    (static_cast<uint32_t>(v3) << 24) | (static_cast<uint32_t>(v2) << 16) | (static_cast<uint32_t>(v1) << 8) | (static_cast<uint32_t>(v0)));
#endif
                        }

                        static inline __m512i set_inc(
                                uint8_t v0) {
#ifdef __AVX512BW__
                            return _mm512_set_epi8(v0 + 63, v0 + 62, v0 + 61, v0 + 60, v0 + 59, v0 + 58, v0 + 57, v0 + 56, v0 + 55, v0 + 54, v0 + 53, v0 + 52, v0 + 51, v0 + 50, v0 + 49, v0 + 48,
                                    v0 + 47, v0 + 46, v0 + 45, v0 + 44, v0 + 43, v0 + 42, v0 + 41, v0 + 40, v0 + 39, v0 + 38, v0 + 37, v0 + 36, v0 + 35, v0 + 34, v0 + 33, v0 + 32, v0 + 31, v0 + 30,
                                    v0 + 29, v0 + 28, v0 + 27, v0 + 26, v0 + 25, v0 + 24, v0 + 23, v0 + 22, v0 + 21, v0 + 20, v0 + 19, v0 + 18, v0 + 17, v0 + 16, v0 + 15, v0 + 14, v0 + 13, v0 + 12,
                                    v0 + 11, v0 + 10, v0 + 9, v0 + 8, v0 + 7, v0 + 6, v0 + 5, v0 + 4, v0 + 3, v0 + 2, v0 + 1, v0);
#else
                            return _mm512_set_epi32(
                                    (static_cast<uint32_t>(v0 + 63) << 24) | (static_cast<uint32_t>(v0 + 62) << 16) | (static_cast<uint32_t>(v0 + 61) << 8) | (static_cast<uint32_t>(v0 + 60)),
                                    (static_cast<uint32_t>(v0 + 59) << 24) | (static_cast<uint32_t>(v0 + 58) << 16) | (static_cast<uint32_t>(v0 + 57) << 8) | (static_cast<uint32_t>(v0 + 56)),
                                    (static_cast<uint32_t>(v0 + 55) << 24) | (static_cast<uint32_t>(v0 + 54) << 16) | (static_cast<uint32_t>(v0 + 53) << 8) | (static_cast<uint32_t>(v0 + 52)),
                                    (static_cast<uint32_t>(v0 + 51) << 24) | (static_cast<uint32_t>(v0 + 50) << 16) | (static_cast<uint32_t>(v0 + 49) << 8) | (static_cast<uint32_t>(v0 + 48)),
                                    (static_cast<uint32_t>(v0 + 47) << 24) | (static_cast<uint32_t>(v0 + 46) << 16) | (static_cast<uint32_t>(v0 + 45) << 8) | (static_cast<uint32_t>(v0 + 44)),
                                    (static_cast<uint32_t>(v0 + 43) << 24) | (static_cast<uint32_t>(v0 + 42) << 16) | (static_cast<uint32_t>(v0 + 41) << 8) | (static_cast<uint32_t>(v0 + 40)),
                                    (static_cast<uint32_t>(v0 + 39) << 24) | (static_cast<uint32_t>(v0 + 38) << 16) | (static_cast<uint32_t>(v0 + 37) << 8) | (static_cast<uint32_t>(v0 + 36)),
                                    (static_cast<uint32_t>(v0 + 35) << 24) | (static_cast<uint32_t>(v0 + 34) << 16) | (static_cast<uint32_t>(v0 + 33) << 8) | (static_cast<uint32_t>(v0 + 32)),
                                    (static_cast<uint32_t>(v0 + 31) << 24) | (static_cast<uint32_t>(v0 + 30) << 16) | (static_cast<uint32_t>(v0 + 29) << 8) | (static_cast<uint32_t>(v0 + 28)),
                                    (static_cast<uint32_t>(v0 + 27) << 24) | (static_cast<uint32_t>(v0 + 26) << 16) | (static_cast<uint32_t>(v0 + 25) << 8) | (static_cast<uint32_t>(v0 + 24)),
                                    (static_cast<uint32_t>(v0 + 23) << 24) | (static_cast<uint32_t>(v0 + 22) << 16) | (static_cast<uint32_t>(v0 + 21) << 8) | (static_cast<uint32_t>(v0 + 20)),
                                    (static_cast<uint32_t>(v0 + 19) << 24) | (static_cast<uint32_t>(v0 + 18) << 16) | (static_cast<uint32_t>(v0 + 17) << 8) | (static_cast<uint32_t>(v0 + 16)),
                                    (static_cast<uint32_t>(v0 + 15) << 24) | (static_cast<uint32_t>(v0 + 14) << 16) | (static_cast<uint32_t>(v0 + 13) << 8) | (static_cast<uint32_t>(v0 + 12)),
                                    (static_cast<uint32_t>(v0 + 11) << 24) | (static_cast<uint32_t>(v0 + 10) << 16) | (static_cast<uint32_t>(v0 + 9) << 8) | (static_cast<uint32_t>(v0 + 8)),
                                    (static_cast<uint32_t>(v0 + 7) << 24) | (static_cast<uint32_t>(v0 + 6) << 16) | (static_cast<uint32_t>(v0 + 5) << 8) | (static_cast<uint32_t>(v0 + 4)),
                                    (static_cast<uint32_t>(v0 + 3) << 24) | (static_cast<uint32_t>(v0 + 2) << 16) | (static_cast<uint32_t>(v0 + 1) << 8) | (static_cast<uint32_t>(v0)));
#endif
                        }

                        static inline __m512i set_inc(
                                uint8_t v0,
                                uint16_t inc) {
#ifdef __AVX512BW__
                            return _mm512_set_epi8(v0 + 63 * inc, v0 + 62 * inc, v0 + 61 * inc, v0 + 60 * inc, v0 + 59 * inc, v0 + 58 * inc, v0 + 57 * inc, v0 + 56 * inc, v0 + 55 * inc, v0 + 54 * inc,
                                    v0 + 53 * inc, v0 + 52 * inc, v0 + 51 * inc, v0 + 50 * inc, v0 + 49 * inc, v0 + 48 * inc, v0 + 47 * inc, v0 + 46 * inc, v0 + 45 * inc, v0 + 44 * inc, v0 + 43 * inc,
                                    v0 + 42 * inc, v0 + 41 * inc, v0 + 40 * inc, v0 + 39 * inc, v0 + 38 * inc, v0 + 37 * inc, v0 + 36 * inc, v0 + 35 * inc, v0 + 34 * inc, v0 + 33 * inc, v0 + 32 * inc,
                                    v0 + 31 * inc, v0 + 30 * inc, v0 + 29 * inc, v0 + 28 * inc, v0 + 27 * inc, v0 + 26 * inc, v0 + 25 * inc, v0 + 24 * inc, v0 + 23 * inc, v0 + 22 * inc, v0 + 21 * inc,
                                    v0 + 20 * inc, v0 + 19 * inc, v0 + 18 * inc, v0 + 17 * inc, v0 + 16 * inc, v0 + 15 * inc, v0 + 14 * inc, v0 + 13 * inc, v0 + 12 * inc, v0 + 11 * inc, v0 + 10 * inc,
                                    v0 + 9 * inc, v0 + 8 * inc, v0 + 7 * inc, v0 + 6 * inc, v0 + 5 * inc, v0 + 4 * inc, v0 + 3 * inc, v0 + 2 * inc, v0 + inc, v0);
#else
                            return _mm512_set_epi32(
                                    (static_cast<uint32_t>(v0 + 63 * inc) << 24) | (static_cast<uint32_t>(v0 + 62 * inc) << 16) | (static_cast<uint32_t>(v0 + 61 * inc) << 8)
                                            | (static_cast<uint32_t>(v0 + 60 * inc)),
                                    (static_cast<uint32_t>(v0 + 59 * inc) << 24) | (static_cast<uint32_t>(v0 + 58 * inc) << 16) | (static_cast<uint32_t>(v0 + 57 * inc) << 8)
                                            | (static_cast<uint32_t>(v0 + 56 * inc)),
                                    (static_cast<uint32_t>(v0 + 55 * inc) << 24) | (static_cast<uint32_t>(v0 + 54 * inc) << 16) | (static_cast<uint32_t>(v0 + 53 * inc) << 8)
                                            | (static_cast<uint32_t>(v0 + 52 * inc)),
                                    (static_cast<uint32_t>(v0 + 51 * inc) << 24) | (static_cast<uint32_t>(v0 + 50 * inc) << 16) | (static_cast<uint32_t>(v0 + 49 * inc) << 8)
                                            | (static_cast<uint32_t>(v0 + 48 * inc)),
                                    (static_cast<uint32_t>(v0 + 47 * inc) << 24) | (static_cast<uint32_t>(v0 + 46 * inc) << 16) | (static_cast<uint32_t>(v0 + 45 * inc) << 8)
                                            | (static_cast<uint32_t>(v0 + 44 * inc)),
                                    (static_cast<uint32_t>(v0 + 43 * inc) << 24) | (static_cast<uint32_t>(v0 + 42 * inc) << 16) | (static_cast<uint32_t>(v0 + 41 * inc) << 8)
                                            | (static_cast<uint32_t>(v0 + 40 * inc)),
                                    (static_cast<uint32_t>(v0 + 39 * inc) << 24) | (static_cast<uint32_t>(v0 + 38 * inc) << 16) | (static_cast<uint32_t>(v0 + 37 * inc) << 8)
                                            | (static_cast<uint32_t>(v0 + 36 * inc)),
                                    (static_cast<uint32_t>(v0 + 35 * inc) << 24) | (static_cast<uint32_t>(v0 + 34 * inc) << 16) | (static_cast<uint32_t>(v0 + 33 * inc) << 8)
                                            | (static_cast<uint32_t>(v0 + 32 * inc)),
                                    (static_cast<uint32_t>(v0 + 31 * inc) << 24) | (static_cast<uint32_t>(v0 + 30 * inc) << 16) | (static_cast<uint32_t>(v0 + 29 * inc) << 8)
                                            | (static_cast<uint32_t>(v0 + 28 * inc)),
                                    (static_cast<uint32_t>(v0 + 27 * inc) << 24) | (static_cast<uint32_t>(v0 + 26 * inc) << 16) | (static_cast<uint32_t>(v0 + 25 * inc) << 8)
                                            | (static_cast<uint32_t>(v0 + 24 * inc)),
                                    (static_cast<uint32_t>(v0 + 23 * inc) << 24) | (static_cast<uint32_t>(v0 + 22 * inc) << 16) | (static_cast<uint32_t>(v0 + 21 * inc) << 8)
                                            | (static_cast<uint32_t>(v0 + 20 * inc)),
                                    (static_cast<uint32_t>(v0 + 19 * inc) << 24) | (static_cast<uint32_t>(v0 + 18 * inc) << 16) | (static_cast<uint32_t>(v0 + 17 * inc) << 8)
                                            | (static_cast<uint32_t>(v0 + 16 * inc)),
                                    (static_cast<uint32_t>(v0 + 15 * inc) << 24) | (static_cast<uint32_t>(v0 + 14 * inc) << 16) | (static_cast<uint32_t>(v0 + 13 * inc) << 8)
                                            | (static_cast<uint32_t>(v0 + 12 * inc)),
                                    (static_cast<uint32_t>(v0 + 11 * inc) << 24) | (static_cast<uint32_t>(v0 + 10 * inc) << 16) | (static_cast<uint32_t>(v0 + 9 * inc) << 8)
                                            | (static_cast<uint32_t>(v0 + 8 * inc)),
                                    (static_cast<uint32_t>(v0 + 7 * inc) << 24) | (static_cast<uint32_t>(v0 + 6 * inc) << 16) | (static_cast<uint32_t>(v0 + 5 * inc) << 8)
                                            | (static_cast<uint32_t>(v0 + 4 * inc)),
                                    (static_cast<uint32_t>(v0 + 3 * inc) << 24) | (static_cast<uint32_t>(v0 + 2 * inc) << 16) | (static_cast<uint32_t>(v0 + 1 * inc) << 8)
                                            | (static_cast<uint32_t>(v0)));
#endif
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

                        static inline __m512i add(
                                __m512i a,
                                __m512i b) {
#ifdef __AVX512BW__
                            return _mm512_add_epi8(a, b);
#else
                            throw std::runtime_error("__m512i add uint8_t not supported yet");
#endif
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
                            Private::pack_right2_uint8(result, a, mask);
                        }

                        static inline void pack_right3(
                                uint8_t * & result,
                                __m512i a,
                                mask_t mask) {
                            typedef mm<__m128i, uint8_t>::mask_t sse_mask_t;
                            auto subMask = static_cast<sse_mask_t>(mask);
                            _mm_storeu_si128(reinterpret_cast<__m128i *>(result), mm<__m128i, uint8_t>::pack_right(_mm512_extracti32x4_epi32(a, 0), subMask));
                            result += __builtin_popcount(subMask);
                            subMask = static_cast<sse_mask_t>(mask >> 16);
                            _mm_storeu_si128(reinterpret_cast<__m128i *>(result), mm<__m128i, uint8_t>::pack_right(_mm512_extracti32x4_epi32(a, 1), subMask));
                            result += __builtin_popcount(subMask);
                            subMask = static_cast<sse_mask_t>(mask >> 32);
                            _mm_storeu_si128(reinterpret_cast<__m128i *>(result), mm<__m128i, uint8_t>::pack_right(_mm512_extracti32x4_epi32(a, 2), subMask));
                            result += __builtin_popcount(subMask);
                            subMask = static_cast<sse_mask_t>(mask >> 48);
                            _mm_storeu_si128(reinterpret_cast<__m128i *>(result), mm<__m128i, uint8_t>::pack_right(_mm512_extracti32x4_epi32(a, 3), subMask));
                            result += __builtin_popcount(subMask);
                        }
                    };

                }
            }
        }
    }
}
