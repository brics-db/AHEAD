/*
 * SSECMP_uint8.tcc
 *
 *  Created on: 26.06.2017
 *      Author: till
 */

#ifndef LIB_COLUMN_OPERATORS_SSE_SSECMP_UINT8_TCC_
#define LIB_COLUMN_OPERATORS_SSE_SSECMP_UINT8_TCC_

#include <immintrin.h>

#include "SSE.hpp"
#include "SSECMP.hpp"
#include <column_operators/functors.hpp>

namespace ahead {
    namespace bat {
        namespace ops {
            namespace sse {

                template<>
                struct v2_mm128_cmp<uint8_t, std::greater> {

                    typedef uint16_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_cmplt_epi8(b, a);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_epi8(cmp(a, b)));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint8_t, std::greater_equal> {

                    typedef uint16_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        auto mm = v2_mm128<uint8_t>::max(a, b);
                        return _mm_cmpeq_epi8(a, mm);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_epi8(cmp(a, b)));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint8_t, std::less> {

                    typedef uint16_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_cmplt_epi8(a, b);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_epi8(cmp(a, b)));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint8_t, std::less_equal> {

                    typedef uint16_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        auto mm = v2_mm128<uint8_t>::min(a, b);
                        return _mm_cmpeq_epi8(a, mm);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_epi8(cmp(a, b)));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint8_t, std::equal_to> {

                    typedef uint16_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_cmpeq_epi8(a, b);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_epi8(cmp(a, b)));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint8_t, std::not_equal_to> {

                    typedef uint16_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_or_si128(_mm_cmplt_epi8(a, b), _mm_cmpgt_epi8(a, b));
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_epi8(cmp(a, b)));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint8_t, ahead::bat::ops::AND> {

                    typedef uint16_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_and_si128(a, b);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_epi8(cmp(a, b)));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint8_t, ahead::bat::ops::OR> {

                    typedef uint16_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_or_si128(a, b);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return static_cast<mask_t>(_mm_movemask_epi8(cmp(a, b)));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint8_t, ahead::bat::ops::ADD> {

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_add_epi8(a, b);
                    }
                };

                template<>
                struct v2_mm128_cmp<uint8_t, ahead::bat::ops::SUB> {

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_sub_epi8(a, b);
                    }
                };

                template<>
                struct v2_mm128_cmp<uint8_t, ahead::bat::ops::MUL> {

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return v2_mm128<uint8_t>::mullo(a, b);
                    }
                };

                template<>
                struct v2_mm128_cmp<uint8_t, ahead::bat::ops::DIV> {

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        auto mm1 = _mm_div_ps(_mm_cvtepi32_ps(_mm_cvtepi8_epi32(a)), _mm_cvtepi32_ps(_mm_cvtepi8_epi32(b)));
                        auto mm2 = _mm_div_ps(_mm_cvtepi32_ps(_mm_cvtepi8_epi32(_mm_srli_si128(a, 4))), _mm_cvtepi32_ps(_mm_cvtepi8_epi32(_mm_srli_si128(b, 4))));
                        auto mm3 = _mm_div_ps(_mm_cvtepi32_ps(_mm_cvtepi8_epi32(_mm_srli_si128(a, 8))), _mm_cvtepi32_ps(_mm_cvtepi8_epi32(_mm_srli_si128(b, 8))));
                        auto mm4 = _mm_div_ps(_mm_cvtepi32_ps(_mm_cvtepi8_epi32(_mm_srli_si128(a, 12))), _mm_cvtepi32_ps(_mm_cvtepi8_epi32(_mm_srli_si128(b, 12))));
                        auto mx1 = _mm_cvtepi32_epi8(_mm_cvtps_epi32(mm1));
                        auto mx2 = _mm_shuffle_epi8(_mm_cvtepi32_epi8(_mm_cvtps_epi32(mm2)), _mm_set_epi64x(0xFFFFFFFFFFFFFFFF, 0x0C080400FFFFFFFF));
                        auto mx3 = _mm_shuffle_epi8(_mm_cvtepi32_epi8(_mm_cvtps_epi32(mm3)), _mm_set_epi64x(0xFFFFFFFF0C080400, 0xFFFFFFFFFFFFFFFF));
                        auto mx4 = _mm_shuffle_epi8(_mm_cvtepi32_epi8(_mm_cvtps_epi32(mm4)), _mm_set_epi64x(0x0C080400FFFFFFFF, 0xFFFFFFFFFFFFFFFF));
                        return _mm_or_si128(_mm_or_si128(mx1, mx2), _mm_or_si128(mx3, mx4));
                    }
                };

            }
        }
    }
}

#endif /* LIB_COLUMN_OPERATORS_SSE_SSECMP_UINT8_TCC_ */
