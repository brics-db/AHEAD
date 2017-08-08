/*
 * SSECMP_uint16.tcc
 *
 *  Created on: 26.06.2017
 *      Author: till
 */

#ifndef LIB_COLUMN_OPERATORS_SSE_SSECMP_UINT16_TCC_
#define LIB_COLUMN_OPERATORS_SSE_SSECMP_UINT16_TCC_

#include <immintrin.h>

#include "SSE.hpp"
#include "SSECMP.hpp"
#include <column_operators/functors.hpp>

namespace ahead {
    namespace bat {
        namespace ops {
            namespace sse {

                namespace Private {

                    inline uint8_t v2_mm128_compact_mask_uint16_t(
                            uint16_t && mask) {
                        mask = mask & 0x5555;
                        mask = ((mask >> 1) | mask) & 0x3333;
                        mask = ((mask >> 2) | mask) & 0x0F0F;
                        mask = ((mask >> 4) | mask) & 0x00FF;
                        return static_cast<uint8_t>(mask);
                    }

                }

                template<>
                struct v2_mm128_cmp<uint16_t, std::greater> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_cmplt_epi16(b, a);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return Private::v2_mm128_compact_mask_uint16_t(static_cast<uint16_t>(_mm_movemask_epi8(cmp(a, b))));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint16_t, std::greater_equal> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        auto mm = v2_mm128<uint16_t>::max(a, b);
                        return _mm_cmpeq_epi16(a, mm);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return Private::v2_mm128_compact_mask_uint16_t(static_cast<uint16_t>(_mm_movemask_epi8(cmp(a, b))));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint16_t, std::less> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_cmplt_epi16(a, b);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return Private::v2_mm128_compact_mask_uint16_t(static_cast<uint16_t>(_mm_movemask_epi8(cmp(a, b))));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint16_t, std::less_equal> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        auto mm = v2_mm128<uint16_t>::min(a, b);
                        return _mm_cmpeq_epi16(a, mm);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return Private::v2_mm128_compact_mask_uint16_t(static_cast<uint16_t>(_mm_movemask_epi8(cmp(a, b))));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint16_t, std::equal_to> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_cmpeq_epi16(a, b);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return Private::v2_mm128_compact_mask_uint16_t(static_cast<uint16_t>(_mm_movemask_epi8(cmp(a, b))));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint16_t, std::not_equal_to> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_or_si128(_mm_cmplt_epi16(a, b), _mm_cmpgt_epi16(a, b));
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return Private::v2_mm128_compact_mask_uint16_t(static_cast<uint16_t>(_mm_movemask_epi8(cmp(a, b))));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint16_t, ahead::bat::ops::AND> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_and_si128(a, b);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return Private::v2_mm128_compact_mask_uint16_t(static_cast<uint16_t>(_mm_movemask_epi8(cmp(a, b))));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint16_t, ahead::bat::ops::OR> {

                    typedef uint8_t mask_t;

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_or_si128(a, b);
                    }

                    static inline mask_t cmp_mask(
                            __m128i a,
                            __m128i b) {
                        return Private::v2_mm128_compact_mask_uint16_t(static_cast<uint16_t>(_mm_movemask_epi8(cmp(a, b))));
                    }
                };

                template<>
                struct v2_mm128_cmp<uint16_t, ahead::bat::ops::ADD> {

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_add_epi16(a, b);
                    }
                };

                template<>
                struct v2_mm128_cmp<uint16_t, ahead::bat::ops::SUB> {

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return _mm_sub_epi16(a, b);
                    }
                };

                template<>
                struct v2_mm128_cmp<uint16_t, ahead::bat::ops::MUL> {

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        return v2_mm128<uint16_t>::mullo(a, b);
                    }
                };

                template<>
                struct v2_mm128_cmp<uint16_t, ahead::bat::ops::DIV> {

                    static inline __m128i cmp(
                            __m128i a,
                            __m128i b) {
                        auto mm1 = _mm_div_ps(_mm_cvtepi32_ps(_mm_cvtepi16_epi32(a)), _mm_cvtepi32_ps(_mm_cvtepi16_epi32(b)));
                        auto mm2 = _mm_div_ps(_mm_cvtepi32_ps(_mm_cvtepi16_epi32(_mm_srli_si128(a, 8))), _mm_cvtepi32_ps(_mm_cvtepi16_epi32(_mm_srli_si128(b, 8))));
                        auto mx1 = _mm_cvtepi32_epi8(_mm_cvtps_epi32(mm1));
                        auto mx2 = _mm_shuffle_epi8(_mm_cvtepi32_epi8(_mm_cvtps_epi32(mm2)), _mm_set_epi64x(0x0D0C090805040100, 0xFFFFFFFFFFFFFFFF));
                        return _mm_or_si128(mx1, mx2);
                    }
                };

            }
        }
    }
}

#endif /* LIB_COLUMN_OPERATORS_SSE_SSECMP_UINT16_TCC_ */
