/*
 * SSECMP_uint64.tcc
 *
 *  Created on: 26.06.2017
 *      Author: till
 */

#ifndef LIB_COLUMN_OPERATORS_SSE_SSECMP_UINT64_TCC_
#define LIB_COLUMN_OPERATORS_SSE_SSECMP_UINT64_TCC_

#include <immintrin.h>

#include "SSE.hpp"
#include "SSECMP.hpp"
#include <column_operators/functors.hpp>

namespace ahead {
    namespace bat {
        namespace ops {
            namespace sse {

                template<>
                struct v2_mm128_cmp<uint64_t, std::greater> {

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
                struct v2_mm128_cmp<uint64_t, std::greater_equal> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        auto mm = v2_mm128<uint64_t>::max(a, b);
                        return _mm_cmpeq_epi64(a, mm);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_pd(_mm_castsi128_pd(cmp(a, b))));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint64_t, std::less> {

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
                struct v2_mm128_cmp<uint64_t, std::less_equal> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        auto mm = v2_mm128<uint64_t>::min(a, b);
                        return _mm_cmpeq_epi64(a, mm);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_pd(_mm_castsi128_pd(cmp(a, b))));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint64_t, std::equal_to> {

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
                struct v2_mm128_cmp<uint64_t, std::not_equal_to> {

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
                struct v2_mm128_cmp<uint64_t, ahead::bat::ops::AND> {

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
                struct v2_mm128_cmp<uint64_t, ahead::bat::ops::OR> {

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
                struct v2_mm128_cmp<uint64_t, ahead::bat::ops::ADD> {

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_add_epi64(a, b);
                    }
                };

                template<>
                struct v2_mm128_cmp<uint64_t, ahead::bat::ops::SUB> {

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_sub_epi64(a, b);
                    }
                };

                template<>
                struct v2_mm128_cmp<uint64_t, ahead::bat::ops::MUL> {

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return v2_mm128<uint64_t>::mullo(a, b);
                    }
                };

                template<>
                struct v2_mm128_cmp<uint64_t, ahead::bat::ops::DIV> {

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_set_epi64x(_mm_extract_epi64(a, 1) / _mm_extract_epi64(b, 1), _mm_extract_epi64(a, 0) / _mm_extract_epi64(b, 0));
                    }
                };

            }
        }
    }
}

#endif /* LIB_COLUMN_OPERATORS_SSE_SSECMP_UINT64_TCC_ */
