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

#ifndef LIB_COLUMN_OPERATORS_SIMD_AVX2_HPP_
#define LIB_COLUMN_OPERATORS_SIMD_AVX2_HPP_

#ifdef __AVX2__

#include <algorithm>
#include <cstdint>
#include <immintrin.h>

#include "SIMD.hpp"

namespace ahead {
    namespace bat {
        namespace ops {
            namespace simd {
                namespace avx2 {

                    template<typename T>
                    struct v2_mm256;

                    namespace Private {
                        template<size_t current = 0>
                        inline void pack_right2_uint8(
                                uint8_t * & result,
                                __m256i & a,
                                uint32_t mask) {
                            *result = reinterpret_cast<uint8_t*>(&a)[current];
                            result += (mask >> current) & 0x1;
                            pack_right2_uint8<current + 1>(result, a, mask);
                        }

                        template<>
                        inline void pack_right2_uint8<31>(
                                uint8_t * & result,
                                __m256i & a,
                                uint32_t mask) {
                            *result = reinterpret_cast<uint8_t*>(&a)[31];
                            result += (mask >> 31) & 0x1;
                        }

                        template<size_t current = 0>
                        inline void pack_right2_uint16(
                                uint16_t * & result,
                                __m256i & a,
                                uint16_t mask) {
                            *result = reinterpret_cast<uint16_t*>(&a)[current];
                            result += (mask >> current) & 0x1;
                            pack_right2_uint16<current + 1>(result, a, mask);
                        }

                        template<>
                        inline void pack_right2_uint16<15>(
                                uint16_t * & result,
                                __m256i & a,
                                uint16_t mask) {
                            *result = reinterpret_cast<uint16_t*>(&a)[15];
                            result += (mask >> 15) & 0x1;
                        }

                        template<size_t current = 0>
                        inline void pack_right2_uint32(
                                uint32_t * & result,
                                __m256i & a,
                                uint8_t mask) {
                            *result = reinterpret_cast<uint32_t*>(&a)[current];
                            result += (mask >> current) & 0x1;
                            pack_right2_uint32<current + 1>(result, a, mask);
                        }

                        template<>
                        inline void pack_right2_uint32<7>(
                                uint32_t * & result,
                                __m256i & a,
                                uint8_t mask) {
                            *result = reinterpret_cast<uint32_t*>(&a)[7];
                            result += (mask >> 7) & 0x1;
                        }

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

                        inline __m256i _mm256_shuffle256_epi8(
                                const __m256i & value,
                                const __m256i & shuffle) {
                            const __m256i K0 = _mm256_setr_epi8(0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0,
                                    0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0);

                            const __m256i K1 = _mm256_setr_epi8(0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70,
                                    0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70);

                            return _mm256_or_si256(_mm256_shuffle_epi8(value, _mm256_add_epi8(shuffle, K0)), _mm256_shuffle_epi8(_mm256_permute4x64_epi64(value, 0x4E), _mm256_add_epi8(shuffle, K1)));
                        }
                    }

                    template<>
                    struct v2_mm256<uint8_t> {

                        typedef uint32_t mask_t;

                        static inline __m256i set1(
                                uint8_t value) {
                            return _mm256_set1_epi8(value);
                        }

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

                        static inline __m256i set_inc(
                                uint8_t v0) {
                            return _mm256_set_epi8(v0 + 31, v0 + 30, v0 + 29, v0 + 28, v0 + 27, v0 + 26, v0 + 25, v0 + 24, v0 + 23, v0 + 22, v0 + 21, v0 + 20, v0 + 19, v0 + 18, v0 + 17, v0 + 16,
                                    v0 + 15, v0 + 14, v0 + 13, v0 + 12, v0 + 11, v0 + 10, v0 + 9, v0 + 8, v0 + 7, v0 + 6, v0 + 5, v0 + 4, v0 + 3, v0 + 2, v0 + 1, v0);
                        }

                        static inline __m256i set_inc(
                                uint8_t v0,
                                uint16_t inc) {
                            return _mm256_set_epi8(v0 + 31 * inc, v0 + 30 * inc, v0 + 29 * inc, v0 + 28 * inc, v0 + 27 * inc, v0 + 26 * inc, v0 + 25 * inc, v0 + 24 * inc, v0 + 23 * inc, v0 + 22 * inc,
                                    v0 + 21 * inc, v0 + 20 * inc, v0 + 19 * inc, v0 + 18 * inc, v0 + 17 * inc, v0 + 16 * inc, v0 + 15 * inc, v0 + 14 * inc, v0 + 13 * inc, v0 + 12 * inc, v0 + 11 * inc,
                                    v0 + 10 * inc, v0 + 9 * inc, v0 + 8 * inc, v0 + 7 * inc, v0 + 6 * inc, v0 + 5 * inc, v0 + 4 * inc, v0 + 3 * inc, v0 + 2 * inc, v0 + inc, v0);
                        }

                        static inline __m256i min(
                                __m256i a,
                                __m256i b) {
                            return _mm256_min_epu8(a, b);
                        }

                        static inline __m256i max(
                                __m256i a,
                                __m256i b) {
                            return _mm256_max_epu8(a, b);
                        }

                        static inline __m256i mullo(
                                __m256i a,
                                __m256i b) {
                            auto res1 = _mm256_mullo_epi16(_mm256_cvtepi8_epi16(_mm256_extractf128_si256(a, 0)), _mm256_cvtepi8_epi16(_mm256_extractf128_si256(b, 0)));
                            res1 = _mm256_shuffle_epi8(res1, _mm256_set_epi64x(0xFFFFFFFFFFFFFFFFull, 0x0D0C090805040100ull, 0xFFFFFFFFFFFFFFFFull, 0x0D0C090805040100ull));
                            auto res2 = _mm256_mullo_epi16(_mm256_cvtepi8_epi16(_mm256_extractf128_si256(a, 1)), _mm256_cvtepi8_epi16(_mm256_extractf128_si256(b, 1)));
                            res2 = _mm256_shuffle_epi8(res2, _mm256_set_epi64x(0xFFFFFFFFFFFFFFFFull, 0x0D0C090805040100ull, 0xFFFFFFFFFFFFFFFFull, 0x0D0C090805040100ull));
                            return _mm256_set_epi64x(_mm256_extract_epi64(res2, 2), _mm256_extract_epi64(res2, 0), _mm256_extract_epi64(res1, 2), _mm256_extract_epi64(res1, 0));
                        }

                        static inline uint8_t sum(
                                __m256i a) {
                            auto mm1 = _mm256_cvtepi8_epi32(_mm256_extractf128_si256(a, 0));
                            auto mm2 = _mm256_cvtepi8_epi32(_mm_srli_si128(_mm256_extractf128_si256(a, 0), 8));
                            auto mm3 = _mm256_cvtepi8_epi32(_mm256_extractf128_si256(a, 1));
                            auto mm4 = _mm256_cvtepi8_epi32(_mm_srli_si128(_mm256_extractf128_si256(a, 1), 8));
                            mm1 = _mm256_add_epi32(mm1, mm2);
                            mm3 = _mm256_add_epi32(mm3, mm4);
                            mm1 = _mm256_add_epi32(mm1, mm3);
                            mm1 = _mm256_add_epi32(mm1, _mm256_srli_si256(mm1, 8));
                            mm1 = _mm256_add_epi32(mm1, _mm256_srli_si256(mm1, 4));
                            auto mm128 = _mm_add_epi32(_mm256_extractf128_si256(mm1, 1), _mm256_extractf128_si256(mm1, 0));
                            return static_cast<uint8_t>(_mm_extract_epi32(mm128, 0));
                        }

                        static inline __m256i pack_right(
                                __m256i a,
                                mask_t mask) {
                            const uint64_t ALL_ONES = 0xFFFFFFFFFFFFFFFFull;
                            int MAX0[2] = {0, 1};
                            int MAX32a[2] = {32, 1}; // used for shifts where we may have to "erase" the argument by shifting it out of the register
                            int MAX32b[2] = {32, 1}; // used for shifts where we may have to "erase" the argument by shifting it out of the register
                            int MAX32c[2] = {32, 1}; // used for shifts where we may have to "erase" the argument by shifting it out of the register
                            int64_t shuffleMaskLL = SHUFFLE_TABLE_LL[static_cast<uint8_t>(mask)];
                            // number of unmatched bytes in bits (if a value matches, the leading bits are zero and the inversion makes it ones, so only full bytes are counted)
                            int clzLL = __builtin_clzll(~shuffleMaskLL);
                            int64_t shuffleMaskLH = SHUFFLE_TABLE_LH[static_cast<uint8_t>(mask >> 8)];
                            int clzLH = __builtin_clzll(~shuffleMaskLH);
                            int64_t shuffleMaskHL = SHUFFLE_TABLE_HL[static_cast<uint8_t>(mask >> 16)];
                            int clzHL = __builtin_clzll(~shuffleMaskHL);
                            int64_t shuffleMaskHH = SHUFFLE_TABLE_HH[static_cast<uint8_t>(mask >> 24)];
                            int clzHH = __builtin_clzll(~shuffleMaskHH);

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
                            MAX32a[1] = clzLL - nSelLH;// amount of right-shift
                            MAX32b[1] = nSelLLLH - 64;// amount of left-shift
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
                                uint8_t * & result,
                                __m256i a,
                                mask_t mask) {
                            if (mask) {
                                Private::pack_right2_uint8(result, a, mask);
                            }
                        }

                    private:
                        static const int64_t * const SHUFFLE_TABLE_LL;
                        static const int64_t * const SHUFFLE_TABLE_LH;
                        static const int64_t * const SHUFFLE_TABLE_HL;
                        static const int64_t * const SHUFFLE_TABLE_HH;
                    };

                    template<>
                    struct v2_mm256<uint16_t> {

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
                            int clzHH = __builtin_clzll(~shuffleMaskHH);

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
                            MAX32a[1] = clzLL - nSelLH;// amount of right-shift
                            MAX32b[1] = nSelLLLH - 64;// amount of left-shift
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

                    template<>
                    struct v2_mm256<uint32_t> {

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

                    template<>
                    struct v2_mm256<uint64_t> {

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

                    private:
                        static const __m256i * const SHUFFLE_TABLE;
                    };

                }

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

            }
        }
    }
}

#endif
