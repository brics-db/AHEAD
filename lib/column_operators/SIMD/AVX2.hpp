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
                    struct mm256;

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

                }
            }
        }
    }
}

#include "AVX2_uint08.tcc"
#include "AVX2_uint16.tcc"
#include "AVX2_uint32.tcc"
#include "AVX2_uint64.tcc"

namespace ahead {
    namespace bat {
        namespace ops {
            namespace simd {

                template<typename T>
                struct mm<__m256i, T> :
                        public avx2::mm256<T> {

                    typedef avx2::mm256<T> BASE;

                    using BASE::set;
                    using BASE::set1;
                    using BASE::set_inc;
                    using BASE::min;
                    using BASE::max;
                    using BASE::add;
                    using BASE::mullo;
                    using BASE::sum;
                    using BASE::pack_right;
                    using BASE::pack_right2;

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

            }
        }
    }
}

#endif
