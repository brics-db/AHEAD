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
 * AVX2_uint32.tcc
 *
 *  Created on: 27.09.2017
 *      Author: Till Kolditz - Till.Kolditz@gmail.com
 */

#pragma once

namespace ahead {
    namespace bat {
        namespace ops {
            namespace simd {
                namespace avx2 {

                    template<>
                    struct mm256<uint32_t> {

                        typedef uint8_t mask_t;

                        static inline __m256i set1(
                                uint32_t value) {
                            return _mm256_set1_epi32(value);
                        }

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

                        static inline __m256i set_inc(
                                uint32_t v0) {
                            return _mm256_set_epi32(v0 + 7, v0 + 6, v0 + 5, v0 + 4, v0 + 3, v0 + 2, v0 + 1, v0);
                        }

                        static inline __m256i set_inc(
                                uint32_t v0,
                                uint32_t inc) {
                            return _mm256_set_epi32(v0 + 7 * inc, v0 + 6 * inc, v0 + 5 * inc, v0 + 4 * inc, v0 + 3 * inc, v0 + 2 * inc, v0 + inc, v0);
                        }

                        static inline __m256i min(
                                __m256i a,
                                __m256i b) {
                            return _mm256_min_epu32(a, b);
                        }

                        static inline __m256i max(
                                __m256i a,
                                __m256i b) {
                            return _mm256_max_epu32(a, b);
                        }

                        static inline __m256i add(
                                __m256i a,
                                __m256i b) {
                            return _mm256_add_epi32(a, b);
                        }

                        static inline __m256i mullo(
                                __m256i a,
                                __m256i b) {
                            return _mm256_mullo_epi32(a, b);
                        }

                        static inline __m256i geq(
                                __m256i a,
                                __m256i b) {
                            auto mm = max(a, b);
                            return _mm256_cmpeq_epi32(a, mm);
                        }

                        static inline uint8_t geq_mask(
                                __m256i a,
                                __m256i b) {
                            return static_cast<uint8_t>(_mm256_movemask_ps(_mm256_castsi256_ps(geq(a, b))));
                        }

                        static inline uint32_t sum(
                                __m256i a) {
                            auto mm = _mm256_add_epi32(a, _mm256_srli_si256(a, 8));
                            mm = _mm256_add_epi32(mm, _mm256_srli_si256(mm, 4));
                            auto mm128 = _mm_add_epi32(_mm256_extractf128_si256(mm, 1), _mm256_extractf128_si256(mm, 0));
                            return static_cast<uint32_t>(_mm_extract_epi32(mm128, 0));
                        }

                        static inline __m256i pack_right(
                                __m256i a,
                                mask_t mask) {
                            return Private::_mm256_shuffle256_epi8(a, SHUFFLE_TABLE[mask]);
                        }

                        static inline void pack_right2(
                                uint32_t * & result,
                                __m256i a,
                                mask_t mask) {
                            if (mask) {
                                Private::pack_right2_uint32(result, a, mask);
                            }
                        }

                    private:
                        static const __m256i * const SHUFFLE_TABLE;
                    };

                }
            }
        }
    }
}
