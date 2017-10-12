/*
 * Copyright 2017 Till Kolditz <till.kolditz@gmail.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * File:   SSEAN.hpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 24. Februar 2017, 14:09
 */

#ifndef SSEAN_HPP
#define SSEAN_HPP

#include <immintrin.h>

#include <column_operators/ANbase.hpp>
#include "../SIMD/SSE.hpp"

namespace ahead {
    namespace bat {
        namespace ops {
            namespace simd {
                namespace sse {

                    namespace Private {

                        template<typename mask_t, size_t step>
                        static inline constexpr void write_out(
                                resoid_t * & pIndicatorPos,
                                resoid_t posEnc,
                                resoid_t AOID,
                                mask_t mask) {
                            *pIndicatorPos = posEnc;
                            pIndicatorPos += mask & 0x1;
                            if constexpr (step > 1) {
                                write_out<mask_t, step - 1>(pIndicatorPos, posEnc + AOID, AOID, mask >> 1);
                            }
                        }

                    }

                    template<typename V, typename T>
                    struct mmAN;

                    template<typename T>
                    struct mmAN<__m128i, T> {

                        static const constexpr size_t units_per_vector = sizeof(__m128i ) / sizeof(T);

                        typedef typename mm128<T>::mask_t mask_t;

                        static inline mask_t detect(
                                __m128i mmCol,
                                __m128i mmInv,
                                __m128i mmDMax,
                                AN_indicator_vector * vec,
                                size_t pos,
                                resoid_t Aoid) {
                            mask_t maskGT = mm_op<__m128i, T, std::greater>::cmp_mask(mm<__m128i, T>::mullo(mmCol, mmInv), mmDMax);
                            if (!maskGT) {
                                return maskGT;
                            } else {
                                decltype(maskGT) test = 1;
                                for (size_t k = 0; k < units_per_vector; ++k, test <<= 1) {
                                    if (maskGT & test) {
                                        vec->push_back((pos + k) * Aoid);
                                    }
                                }
                                return maskGT;
                            }
                        }

                        static inline mask_t detect(
                                __m128i & mmDec,
                                __m128i mmCol,
                                __m128i mmInv,
                                __m128i mmDMax,
                                AN_indicator_vector * vec,
                                size_t pos,
                                resoid_t Aoid) {
                            mmDec = mm<__m128i, T>::mullo(mmCol, mmInv);
                            mask_t maskGT = mm_op<__m128i, T, std::greater>::cmp_mask(mmDec, mmDMax);
                            if (!maskGT) {
                                return maskGT;
                            } else {
                                decltype(maskGT) test = 1;
                                for (size_t k = 0; k < units_per_vector; ++k, test <<= 1) {
                                    if (maskGT & test) {
                                        vec->push_back((pos + k) * Aoid);
                                    }
                                }
                                return maskGT;
                            }
                        }

                        static inline void detect(
                                __m128i & mmCol,
                                __m128i & mmInv,
                                __m128i & mmDMax,
                                resoid_t * & pIndicatorPos,
                                resoid_t & posEnc,
                                resoid_t AOID) {
                            mask_t mask = mm_op<__m128i, T, std::greater>::cmp_mask(mm<__m128i, T>::mullo(mmCol, mmInv), mmDMax);
                            if (!mask) {
                            } else {
                                Private::write_out<mask_t, units_per_vector>(pIndicatorPos, posEnc, AOID, mask);
                            }
                        }

                    };

                }
            }
        }
    }
}

#endif /* SSEAN_HPP */
