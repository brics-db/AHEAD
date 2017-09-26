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

                    template<>
                    struct mm128<uint64_t> {

                        typedef uint8_t mask_t;

                        static inline __m128i set1(
                                uint64_t value) {
                            return _mm_set1_epi64x(value);
                        }

                        static inline __m128i set(
                                uint64_t v1,
                                uint64_t v0) {
                            return _mm_set_epi64x(v1, v0);
                        }

                        static inline __m128i set_inc(
                                uint64_t v0) {
                            return _mm_set_epi64x(v0 + 1, v0);
                        }

                        static inline __m128i set_inc(
                                uint64_t v0,
                                uint64_t inc) {
                            return _mm_set_epi64x(v0 + inc, v0);
                        }

                        static inline __m128i min(
                                __m128i a,
                                __m128i b) {
                            return _mm_set_epi64x(std::min(static_cast<uint64_t>(_mm_extract_epi64(a, 1)), static_cast<uint64_t>(_mm_extract_epi64(b, 1))),
                                    std::min(static_cast<uint64_t>(_mm_extract_epi64(a, 0)), static_cast<uint64_t>(_mm_extract_epi64(b, 0))));
                        }

                        static inline __m128i max(
                                __m128i a,
                                __m128i b) {
                            return _mm_set_epi64x(std::max(static_cast<uint64_t>(_mm_extract_epi64(a, 1)), static_cast<uint64_t>(_mm_extract_epi64(b, 1))),
                                    std::max(static_cast<uint64_t>(_mm_extract_epi64(a, 0)), static_cast<uint64_t>(_mm_extract_epi64(b, 0))));
                        }

                        static inline __m128i add(
                                __m128i a,
                                __m128i b) {
                            return _mm_add_epi64(a, b);
                        }

                        static inline uint64_t sum(
                                __m128i a) {
                            return static_cast<uint64_t>(_mm_extract_epi64(a, 0)) + static_cast<uint64_t>(_mm_extract_epi64(a, 1));
                        }

                        static inline __m128i mullo(
                                __m128i a,
                                __m128i b) {
                            return _mm_set_epi64x(_mm_extract_epi64(a, 1) * _mm_extract_epi64(b, 1), _mm_extract_epi64(a, 0) * _mm_extract_epi64(b, 0));
                        }

                        static inline __m128i pack_right(
                                __m128i a,
                                mask_t mask) {
                            return _mm_shuffle_epi8(a, SHUFFLE_TABLE[mask]);
                        }

                        static inline void pack_right2(
                                uint64_t * & result,
                                __m128i a,
                                mask_t mask) {
                            Private::pack_right2_uint64(result, a, mask);
                        }

                    private:
                        static const __m128i * const SHUFFLE_TABLE;
                    };

                    template<>
                    struct mm128<uint64_t, uint16_t, uint64_t> {

                        static inline __m128i mul_add(
                                __m128i * a,
                                __m128i * b,
                                size_t & incA,
                                size_t & incB) {
                            return mm128<uint16_t, uint64_t, uint64_t>::mul_add(b, a, incB, incA);
                        }

                        template<size_t firstA, size_t firstB>
                        static inline __m128i mullo(
                                __m128i & a,
                                __m128i & b) {
                            return mm128<uint16_t, uint64_t, uint64_t>::mullo<firstB, firstA>(b, a);
                        }
                    };

                    template<>
                    struct mm128<uint64_t, uint32_t, uint64_t> {
                        template<size_t firstA, size_t firstB>
                        static inline __m128i mullo(
                                __m128i & a,
                                __m128i & b) {
                            return mm128<uint32_t, uint64_t, uint64_t>::mullo<firstB, firstA>(b, a);
                        }
                    };

                    template<>
                    struct mm128<uint64_t, uint64_t, uint64_t> {

                        static inline __m128i mul_add(
                                __m128i * a,
                                __m128i * b,
                                size_t & incA,
                                size_t & incB) {
                            incA = incB = 1;
                            return _mm_mullo_epi64(_mm_lddqu_si128(a), _mm_lddqu_si128(b));
                        }
                    };

                    template<>
                    struct mm128<uint64_t, uint32_t> {

                        static inline __m128i convert(
                                __m128i & mm) {
                            return _mm_shuffle_epi8(mm, _mm_set_epi64x(0xFFFFFFFFFFFFFFFF, 0x0B0A090803020100));
                        }
                    };

                    template<>
                    struct mm128<uint64_t, uint64_t> {

                        static inline __m128i convert(
                                __m128i & mm) {
                            return mm;
                        }
                    };

                }

                template<>
                struct mm_op<__m128i, uint64_t, std::greater> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_cmpgt_epi64(a, b);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_pd(_mm_castsi128_pd(cmp(a, b))));
                    }
                };

                template<>
                struct mm_op<__m128i, uint64_t, std::greater_equal> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        auto mm = sse::mm128<uint64_t>::max(a, b);
                        return _mm_cmpeq_epi64(a, mm);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_pd(_mm_castsi128_pd(cmp(a, b))));
                    }
                };

                template<>
                struct mm_op<__m128i, uint64_t, std::less> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_cmpgt_epi64(b, a);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_pd(_mm_castsi128_pd(cmp(a, b))));
                    }
                };

                template<>
                struct mm_op<__m128i, uint64_t, std::less_equal> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        auto mm = sse::mm128<uint64_t>::min(a, b);
                        return _mm_cmpeq_epi64(a, mm);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_pd(_mm_castsi128_pd(cmp(a, b))));
                    }
                };

                template<>
                struct mm_op<__m128i, uint64_t, std::equal_to> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_cmpeq_epi64(a, b);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_pd(_mm_castsi128_pd(cmp(a, b))));
                    }
                };

                template<>
                struct mm_op<__m128i, uint64_t, std::not_equal_to> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_or_si128(_mm_cmpgt_epi64(a, b), _mm_cmpgt_epi64(b, a));
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_pd(_mm_castsi128_pd(cmp(a, b))));
                    }
                };

                template<>
                struct mm_op<__m128i, uint64_t, ahead::and_is> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_and_si128(a, b);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_pd(_mm_castsi128_pd(cmp(a, b))));
                    }
                };

                template<>
                struct mm_op<__m128i, uint64_t, ahead::or_is> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_or_si128(a, b);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_pd(_mm_castsi128_pd(cmp(a, b))));
                    }
                };

                template<>
                struct mm_op<__m128i, uint64_t, ahead::add> {

                    static inline __m128i compute(
                            __m128i a,
                            __m128i b) {
                        return add(a, b);
                    }

                    static inline __m128i add(
                            __m128i a,
                            __m128i b) {
                        return _mm_add_epi64(a, b);
                    }
                };

                template<>
                struct mm_op<__m128i, uint64_t, ahead::sub> {

                    static inline __m128i compute(
                            __m128i a,
                            __m128i b) {
                        return sub(a, b);
                    }

                    static inline __m128i sub(
                            __m128i a,
                            __m128i b) {
                        return _mm_sub_epi64(a, b);
                    }
                };

                template<>
                struct mm_op<__m128i, uint64_t, ahead::mul> {

                    static inline __m128i compute(
                            __m128i a,
                            __m128i b) {
                        return mullo(a, b);
                    }

                    static inline __m128i mullo(
                            __m128i a,
                            __m128i b) {
                        return sse::mm128<uint64_t>::mullo(a, b);
                    }
                };

                template<>
                struct mm_op<__m128i, uint64_t, ahead::div> {

                    static inline __m128i compute(
                            __m128i a,
                            __m128i b) {
                        return div(a, b);
                    }

                    static inline __m128i div(
                            __m128i a,
                            __m128i b) {
                        return _mm_set_epi64x(_mm_extract_epi64(a, 1) / _mm_extract_epi64(b, 1), _mm_extract_epi64(a, 0) / _mm_extract_epi64(b, 0));
                    }
                };

            }
        }
    }
}
