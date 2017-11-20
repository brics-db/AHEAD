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
 * AVX2_uint64.tcc
 *
 *  Created on: 27.09.2017
 *      Author: Till Kolditz - Till.Kolditz@gmail.com
 */

#pragma once

#ifndef LIB_COLUMN_OPERATORS_SIMD_AVX2_HPP_
#error "This file must only be included by AVX2.hpp !"
#endif

namespace ahead {
    namespace bat {
        namespace ops {
            namespace simd {
                namespace avx2 {

                    namespace Private {
                        template<size_t current = 0>
                        inline void pack_right2_uint64(
                                uint64_t * & result,
                                __m256i & a,
                                uint8_t mask) {
                            *result = reinterpret_cast<uint64_t*>(&a)[current];
                            result += (mask >> current) & 0x1;
                            pack_right2_uint64<current + 1>(result, a, mask);
                        }

                        template<>
                        inline void pack_right2_uint64<3>(
                                uint64_t * & result,
                                __m256i & a,
                                uint8_t mask) {
                            *result = reinterpret_cast<uint64_t*>(&a)[3];
                            result += (mask >> 3) & 0x1;
                        }
                    }

                    template<>
                    struct mm256<uint64_t> {

                        typedef uint8_t mask_t;

                        static inline __m256i set1(
                                uint64_t value) {
                            return _mm256_set1_epi64x(value);
                        }

                        static inline __m256i set(
                                uint64_t v3,
                                uint64_t v2,
                                uint64_t v1,
                                uint64_t v0) {
                            return _mm256_set_epi64x(v3, v2, v1, v0);
                        }

                        static inline __m256i set_inc(
                                uint64_t v0) {
                            return _mm256_set_epi64x(v0 + 3, v0 + 2, v0 + 1, v0);
                        }

                        static inline __m256i set_inc(
                                uint64_t v0,
                                uint64_t inc) {
                            return _mm256_set_epi64x(v0 + 3 * inc, v0 + 2 * inc, v0 + inc, v0);
                        }

                        static inline __m256i min(
                                __m256i a,
                                __m256i b) {
                            return _mm256_min_epi64(a, b);
                        }

                        static inline __m256i max(
                                __m256i a,
                                __m256i b) {
                            return _mm256_max_epi64(a, b);
                        }

                        static inline __m256i add(
                                __m256i a,
                                __m256i b) {
                            return _mm256_add_epi64(a, b);
                        }

                        static inline __m256i mullo(
                                __m256i a,
                                __m256i b) {
                            return _mm256_mullo_epi64(a, b);
                        }

                        static inline __m256i geq(
                                __m256i a,
                                __m256i b) {
                            auto mm = max(a, b);
                            return _mm256_cmpeq_epi64(a, mm);
                        }

                        static inline uint8_t geq_mask(
                                __m256i a,
                                __m256i b) {
                            return static_cast<uint8_t>(_mm256_movemask_pd(_mm256_castsi256_pd(geq(a, b))));
                        }

                        static inline uint64_t sum(
                                __m256i a) {
                            auto mm = _mm256_add_epi64(a, _mm256_srli_si256(a, 8));
                            return static_cast<uint64_t>(_mm256_extract_epi64(mm, 0)) + static_cast<uint64_t>(_mm256_extract_epi64(mm, 2));
                        }

                        static inline __m256i pack_right(
                                __m256i a,
                                mask_t mask) {
                            return Private::_mm256_shuffle256_epi8(a, SHUFFLE_TABLE[mask]);
                        }

                        static inline void pack_right2(
                                uint64_t * & result,
                                __m256i a,
                                mask_t mask) {
                            if (mask) {
                                Private::pack_right2_uint64(result, a, mask);
                            }
                        }

                        static inline void pack_right3(
                                uint64_t * & result,
                                __m256i a,
                                mask_t mask) {
                            typedef mm<__m128i, uint64_t>::mask_t sse_mask_t;
                            auto maskLow = static_cast<sse_mask_t>(mask & 0x3);
                            _mm_storeu_si128(reinterpret_cast<__m128i *>(result), mm<__m128i, uint32_t>::pack_right(_mm256_extracti128_si256(a, 0), maskLow));
                            result += __builtin_popcount(maskLow);
                            auto maskHigh = static_cast<sse_mask_t>(mask >> 2);
                            _mm_storeu_si128(reinterpret_cast<__m128i *>(result), mm<__m128i, uint32_t>::pack_right(_mm256_extracti128_si256(a, 1), maskHigh));
                            result += __builtin_popcount(maskHigh);
                        }

                    private:
                        static const __m256i * const SHUFFLE_TABLE;
                    };

                }
            }
        }
    }
}
