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
 * SSE_uint8.tcc
 *
 *  Created on: 25.09.2017
 *      Author: Till Kolditz - Till.Kolditz@gmail.com
 */

#pragma once

#ifndef LIB_COLUMN_OPERATORS_SIMD_SSE_HPP_
#error "This file must only be included by SSE.hpp !"
#endif

namespace ahead {
    namespace bat {
        namespace ops {
            namespace simd {
                namespace sse {

                    namespace Private {

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

                    }

                    template<>
                    struct mm128<uint32_t> {

                        typedef uint8_t mask_t;

                        static inline __m128i set1(
                                uint32_t value) {
                            return _mm_set1_epi32(value);
                        }

                        static inline __m128i set(
                                uint32_t v3,
                                uint32_t v2,
                                uint32_t v1,
                                uint32_t v0) {
                            return _mm_set_epi32(v3, v2, v1, v0);
                        }

                        static inline __m128i set_inc(
                                uint32_t v0) {
                            return _mm_set_epi32(v0 + 3, v0 + 2, v0 + 1, v0);
                        }

                        static inline __m128i set_inc(
                                uint32_t v0,
                                uint32_t inc) {
                            return _mm_set_epi32(v0 + 3 * inc, v0 + 2 * inc, v0 + inc, v0);
                        }

                        static inline __m128i min(
                                __m128i a,
                                __m128i b) {
                            return _mm_min_epu32(a, b);
                        }

                        static inline __m128i max(
                                __m128i a,
                                __m128i b) {
                            return _mm_max_epu32(a, b);
                        }

                        static inline __m128i add(
                                __m128i a,
                                __m128i b) {
                            return _mm_add_epi32(a, b);
                        }

                        static inline uint32_t sum(
                                __m128i a) {
                            auto mm = _mm_add_epi32(a, _mm_srli_si128(a, 8));
                            mm = _mm_add_epi32(mm, _mm_srli_si128(mm, 4));
                            return static_cast<uint32_t>(_mm_extract_epi32(mm, 0));
                        }

                        static inline __m128i mullo(
                                __m128i a,
                                __m128i b) {
                            return _mm_mullo_epi32(a, b);
                        }

                        static inline __m128i pack_right(
                                __m128i a,
                                mask_t mask) {
                            return _mm_shuffle_epi8(a, SHUFFLE_TABLE[mask]);
                        }

                        static inline void pack_right2(
                                uint32_t * & result,
                                __m128i a,
                                mask_t mask) {
                            Private::pack_right2_uint32(result, a, mask);
                        }

                    private:
                        static const __m128i * const SHUFFLE_TABLE;
                    };

                    template<>
                    struct mm128<uint32_t, uint16_t, uint32_t> {

                        static inline __m128i mul_add(
                                __m128i * a,
                                __m128i * b,
                                size_t & incA,
                                size_t & incB) {
                            return mm128<uint16_t, uint32_t, uint32_t>::mul_add(b, a, incB, incA);
                        }
                    };

                    template<>
                    struct mm128<uint32_t, uint16_t, uint64_t> {

                        static inline __m128i mul_add(
                                __m128i * a,
                                __m128i * b,
                                size_t & incA,
                                size_t & incB) {
                            return mm128<uint16_t, uint32_t, uint64_t>::mul_add(b, a, incB, incA);
                        }
                    };

                    template<>
                    struct mm128<uint32_t, uint8_t, uint64_t> {

                        static inline __m128i mul_add(
                                __m128i * a,
                                __m128i * b,
                                size_t & incA,
                                size_t & incB) {
                            incA = 4;
                            incB = 1;

                            auto pA = reinterpret_cast<uint32_t*>(a);
                            auto pB = reinterpret_cast<uint8_t*>(b);
                            uint64_t r1 = 0, r2 = 0;
                            for (size_t i = 0; i < 16; i += 2) {
                                r1 += static_cast<uint64_t>(pA[i]) * static_cast<uint64_t>(pB[i]);
                                r2 += static_cast<uint64_t>(pA[i + 1]) * static_cast<uint64_t>(pB[i + 1]);
                            }
                            __m128i mm = _mm_set_epi64x(r1, r2);

                            return mm;
                        }
                    };

                    template<>
                    struct mm128<uint32_t, uint32_t, uint32_t> {

                        static inline __m128i mul_add(
                                __m128i * a,
                                __m128i * b,
                                size_t & incA,
                                size_t & incB) {
                            incA = incB = 1;
                            return _mm_mullo_epi32(_mm_lddqu_si128(a), _mm_lddqu_si128(b));
                        }
                    };

                    template<>
                    struct mm128<uint32_t, uint32_t, uint64_t> {

                        static inline __m128i mul_add(
                                __m128i * a,
                                __m128i * b,
                                size_t & incA,
                                size_t & incB) {
                            incA = incB = 1;
                            auto mm = _mm_mul_epu32(*a, *b);
                            return _mm_add_epi32(mm, _mm_mul_epu32(_mm_srli_si128(_mm_lddqu_si128(a), 8), _mm_srli_si128(_mm_lddqu_si128(b), 8)));
                        }
                    };

                    template<>
                    struct mm128<uint32_t, uint8_t> {

                        static inline __m128i convert(
                                __m128i & mm) {
                            return _mm_shuffle_epi8(mm, _mm_set_epi64x(0xFFFFFFFFFFFFFFFF, 0xFFFFFFFF0C080400));
                        }
                    };

                    template<>
                    struct mm128<uint32_t, uint16_t> {

                        static inline __m128i convert(
                                __m128i & mm) {
                            return _mm_shuffle_epi8(mm, _mm_set_epi64x(0xFFFFFFFFFFFFFFFF, 0x0D0C090805040100));
                        }
                    };

                    template<>
                    struct mm128<uint32_t, uint32_t> {

                        static inline __m128i convert(
                                __m128i & mm) {
                            return mm;
                        }
                    };

                    template<>
                    struct mm128<uint32_t, uint64_t, uint64_t> {

                        template<size_t firstA, size_t firstB>
                        static inline __m128i mullo(
                                __m128i & a,
                                __m128i & b) {
                            auto r0 = static_cast<uint64_t>(_mm_extract_epi32(a, firstA)) * _mm_extract_epi64(b, firstB);
                            auto r1 = static_cast<uint64_t>(_mm_extract_epi32(a, firstA + 1)) * _mm_extract_epi64(b, firstB + 1);
                            return _mm_set_epi64x(r1, r0);
                        }
                    };

                }

                template<>
                struct mm_op<__m128i, uint32_t, std::greater> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_cmpgt_epi32(a, b);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_ps(_mm_castsi128_ps(cmp(a, b))));
                    }
                };

                template<>
                struct mm_op<__m128i, uint32_t, std::greater_equal> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_cmpeq_epi32(a, sse::mm128<uint32_t>::max(a, b));
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_ps(_mm_castsi128_ps(cmp(a, b))));
                    }
                };

                template<>
                struct mm_op<__m128i, uint32_t, std::less> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_cmplt_epi32(a, b);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_ps(_mm_castsi128_ps(cmp(a, b))));
                    }
                };

                template<>
                struct mm_op<__m128i, uint32_t, std::less_equal> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_cmpeq_epi32(a, sse::mm128<uint32_t>::min(a, b));
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_ps(_mm_castsi128_ps(cmp(a, b))));
                    }
                };

                template<>
                struct mm_op<__m128i, uint32_t, std::equal_to> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_cmpeq_epi32(a, b);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_ps(_mm_castsi128_ps(cmp(a, b))));
                    }
                };

                template<>
                struct mm_op<__m128i, uint32_t, std::not_equal_to> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_or_si128(_mm_cmplt_epi32(a, b), _mm_cmpgt_epi32(a, b));
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_ps(_mm_castsi128_ps(cmp(a, b))));
                    }
                };

                template<>
                struct mm_op<__m128i, uint32_t, ahead::and_is> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_and_si128(a, b);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_ps(_mm_castsi128_ps(cmp(a, b))));
                    }
                };

                template<>
                struct mm_op<__m128i, uint32_t, ahead::or_is> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_or_si128(a, b);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_ps(_mm_castsi128_ps(cmp(a, b))));
                    }
                };

                template<>
                struct mm_op<__m128i, uint32_t, ahead::add> {

                    static inline __m128i compute(
                            __m128i a,
                            __m128i b) {
                        return add(a, b);
                    }

                    static inline __m128i add(
                            __m128i a,
                            __m128i b) {
                        return _mm_add_epi32(a, b);
                    }
                };

                template<>
                struct mm_op<__m128i, uint32_t, ahead::sub> {

                    static inline __m128i compute(
                            __m128i a,
                            __m128i b) {
                        return sub(a, b);
                    }

                    static inline __m128i sub(
                            __m128i a,
                            __m128i b) {
                        return _mm_sub_epi32(a, b);
                    }
                };

                template<>
                struct mm_op<__m128i, uint32_t, ahead::mul> {

                    static inline __m128i compute(
                            __m128i a,
                            __m128i b) {
                        return mullo(a, b);
                    }

                    static inline __m128i mullo(
                            __m128i a,
                            __m128i b) {
                        return sse::mm128<uint32_t>::mullo(a, b);
                    }
                };

                template<>
                struct mm_op<__m128i, uint32_t, ahead::div> {

                    static inline __m128i compute(
                            __m128i a,
                            __m128i b) {
                        return div(a, b);
                    }

                    static inline __m128i div(
                            __m128i a,
                            __m128i b) {
                        return _mm_cvtps_epi32(_mm_div_ps(_mm_cvtepi32_ps(a), _mm_cvtepi32_ps(b)));
                    }
                };

            }
        }
    }
}
