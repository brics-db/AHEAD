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
 * AVX2_uint16.tcc
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
                    struct mm256<uint16_t> {

                        typedef uint16_t mask_t;

                        static inline __m256i set1(
                                uint16_t value) {
                            return _mm256_set1_epi16(value);
                        }

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

                        static inline __m256i set_inc(
                                uint16_t v0) {
                            return _mm256_set_epi16(v0 + 15, v0 + 14, v0 + 13, v0 + 12, v0 + 11, v0 + 10, v0 + 9, v0 + 8, v0 + 7, v0 + 6, v0 + 5, v0 + 4, v0 + 3, v0 + 2, v0 + 1, v0);
                        }

                        static inline __m256i set_inc(
                                uint16_t v0,
                                uint16_t inc) {
                            return _mm256_set_epi16(v0 + 15 * inc, v0 + 14 * inc, v0 + 13 * inc, v0 + 12 * inc, v0 + 11 * inc, v0 + 10 * inc, v0 + 9 * inc, v0 + 8 * inc, v0 + 7 * inc, v0 + 6 * inc,
                                    v0 + 5 * inc, v0 + 4 * inc, v0 + 3 * inc, v0 + 2 * inc, v0 + inc, v0);
                        }

                        static inline __m256i min(
                                __m256i a,
                                __m256i b) {
                            return _mm256_min_epu16(a, b);
                        }

                        static inline __m256i max(
                                __m256i a,
                                __m256i b) {
                            return _mm256_max_epu16(a, b);
                        }

                        static inline __m256i add(
                                __m256i a,
                                __m256i b) {
                            return _mm256_add_epi16(a, b);
                        }

                        static inline __m256i mullo(
                                __m256i a,
                                __m256i b) {
                            return _mm256_mullo_epi16(a, b);
                        }

                        static inline uint16_t sum(
                                __m256i a) {
                            auto mm1 = _mm256_cvtepi16_epi32(_mm256_extractf128_si256(a, 0));
                            auto mm2 = _mm256_cvtepi16_epi32(_mm256_extractf128_si256(a, 1));
                            mm1 = _mm256_add_epi32(mm1, mm2);
                            mm1 = _mm256_add_epi32(mm1, _mm256_srli_si256(mm1, 8));
                            mm1 = _mm256_add_epi32(mm1, _mm256_srli_si256(mm1, 4));
                            auto mm128 = _mm_add_epi32(_mm256_extractf128_si256(mm1, 1), _mm256_extractf128_si256(mm1, 0));
                            return static_cast<uint16_t>(_mm_extract_epi32(mm128, 0));
                        }

                        static inline __m256i pack_right(
                                __m256i a,
                                mask_t mask) {
                            static const uint64_t ALL_ONES = 0xFFFFFFFFFFFFFFFFull;
                            int MAX0[2] = {0, 1};
                            int MAX32a[2] = {32, 1}; // used for shifts where we may have to "erase" the argument by shifting it out of the register
                            int MAX32b[2] = {32, 1}; // used for shifts where we may have to "erase" the argument by shifting it out of the register
                            int MAX32c[2] = {32, 1}; // used for shifts where we may have to "erase" the argument by shifting it out of the register
                            auto mmShuffleMaskL = SHUFFLE_TABLE_L[static_cast<uint8_t>(mask)];
                            auto mmShuffleMaskH = SHUFFLE_TABLE_H[static_cast<uint8_t>(mask >> 8)];
                            int64_t shuffleMaskLL = static_cast<int64_t>(_mm_extract_epi64(mmShuffleMaskL, 0));
                            // number of unmatched bytes in bits (if a value matches, the leading bits are zero and the inversion makes it ones, so only full bytes are counted)
                            int clzLL = __builtin_clzll(~shuffleMaskLL);
                            int64_t shuffleMaskLH = static_cast<int64_t>(_mm_extract_epi64(mmShuffleMaskL, 1));
                            int clzLH = __builtin_clzll(~shuffleMaskLH);
                            int64_t shuffleMaskHL = static_cast<int64_t>(_mm_extract_epi64(mmShuffleMaskH, 0));
                            int clzHL = __builtin_clzll(~shuffleMaskHL);
                            int64_t shuffleMaskHH = static_cast<int64_t>(_mm_extract_epi64(mmShuffleMaskH, 1));

                            int nSelLL = 64 - clzLL;
                            int nSelLH = 64 - clzLH;
                            int nSelHL = 64 - clzHL;
                            int nSelLLLH = nSelLL + nSelLH;
                            MAX0[1] = 64 - nSelLLLH;
                            int clzLLLH = MAX0[MAX0[1] > 0];
                            int nSelLLLHHL = nSelLL + nSelLH + nSelHL;
                            MAX0[1] = 64 - nSelLLLHHL;
                            int clzLLLHHL = MAX0[MAX0[1] > 0];
                            int64_t shufMask0_sub0 = shuffleMaskLL;
                            int64_t shufMask0_sub1 = ((shuffleMaskLH << nSelLL) | (ALL_ONES >> clzLL));
                            int64_t shufMask0_sub2 = ((shuffleMaskHL << nSelLLLH) | (ALL_ONES >> clzLLLH));
                            int64_t shufMask0_sub3 = ((shuffleMaskHH << nSelLLLHHL) | (ALL_ONES >> clzLLLHHL));
                            int64_t shufMask0 = shufMask0_sub0 & shufMask0_sub1 & shufMask0_sub2 & shufMask0_sub3;

                            int64_t shufMask1_sub0 = shuffleMaskLH >> clzLL;
                            MAX32a[1] = clzLL - nSelLH; // amount of right-shift
                            MAX32b[1] = nSelLLLH - 64; // amount of left-shift
                            MAX32c[1] = 128 - nSelLLLH;
                            int64_t shufMask1_sub1 = (shuffleMaskHL >> MAX32a[MAX32a[1] > 0]) | (shuffleMaskHL << MAX32b[MAX32b[1] > 0]) | (ALL_ONES >> MAX32c[MAX32c[1] > 0]);
                            MAX32a[1] = clzLL - (nSelLH + nSelHL);
                            MAX32b[1] = nSelLLLHHL - 64;
                            MAX32c[1] = 128 - nSelLLLHHL;
                            int64_t shufMask1_sub2 = (shuffleMaskHH >> MAX32a[MAX32a[1] > 0]) | (shuffleMaskHH << MAX32b[MAX32b[1] > 0]) | (ALL_ONES >> MAX32c[MAX32c[1] > 0]);
                            int64_t shufMask1 = shufMask1_sub0 & shufMask1_sub1 & shufMask1_sub2;

                            int64_t shufMask2_sub0 = shuffleMaskHL >> (clzLL + clzLH);
                            MAX32a[1] = (clzLL + clzLH) - (nSelLH + nSelHL);
                            MAX32b[1] = nSelLLLHHL - 128;
                            MAX32c[1] = 192 - nSelLLLHHL;
                            int64_t shufMask2_sub1 = (shuffleMaskHH >> MAX32a[MAX32a[1] > 0]) | (shuffleMaskHH << MAX32b[MAX32b[1] > 0]) | (ALL_ONES >> MAX32c[MAX32c[1] > 0]);
                            int64_t shufMask2 = shufMask2_sub0 & shufMask2_sub1;

                            int64_t shufMask3 = shuffleMaskHH >> (clzLL + clzLH + clzHL);
                            return Private::_mm256_shuffle256_epi8(a, _mm256_set_epi64x(shufMask3, shufMask2, shufMask1, shufMask0));
                        }

                        static inline void pack_right2(
                                uint16_t * & result,
                                __m256i a,
                                mask_t mask) {
                            if (mask) {
                                Private::pack_right2_uint16(result, a, mask);
                            }
                        }

                    private:
                        static const __m128i * const SHUFFLE_TABLE_L;
                        static const __m128i * const SHUFFLE_TABLE_H;
                    };
                }
            }
        }
    }
}
