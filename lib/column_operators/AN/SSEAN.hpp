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
#include "../SIMD/SSECMP.hpp"

namespace ahead {
    namespace bat {
        namespace ops {
            namespace sse {

                template<typename T>
                struct v2_mm128_AN {

                    constexpr static const size_t steps = sizeof(__m128i ) / sizeof(T);

                    typedef typename v2_mm128_cmp<T, std::greater>::mask_t mask_t;

                    static inline mask_t detect(
                            __m128i mmCol,
                            __m128i mmInv,
                            __m128i mmDMax,
                            std::vector<bool> * vec,
                            size_t pos) {
                        mask_t maskGT = v2_mm128_cmp<T, std::greater>::cmp_mask(v2_mm128<T>::mullo(mmCol, mmInv), mmDMax);
                        if (maskGT) {
                            // TODO we need a different bit vector implementation where we can store whole masks and not only single boolean values!
                            decltype(maskGT) test = 1;
                            for (size_t k = 0; k < steps; ++k, test <<= 1) {
                                if (maskGT & test) {
                                    (*vec)[pos + k] = true;
                                }
                            }
                        }
                        return maskGT;
                    }

                    static inline mask_t detect(
                            __m128i mmCol,
                            __m128i mmInv,
                            __m128i mmDMax,
                            AN_indicator_vector * vec,
                            size_t pos,
                            resoid_t Aoid) {
                        mask_t maskGT = v2_mm128_cmp<T, std::greater>::cmp_mask(v2_mm128<T>::mullo(mmCol, mmInv), mmDMax);
                        if (maskGT) {
                            decltype(maskGT) test = 1;
                            for (size_t k = 0; k < steps; ++k, test <<= 1) {
                                if (maskGT & test) {
                                    vec->push_back((pos + k) * Aoid);
                                }
                            }
                        }
                        return maskGT;
                    }

                    static inline mask_t detect(
                            __m128i & mmDec,
                            __m128i mmCol,
                            __m128i mmInv,
                            __m128i mmDMax,
                            std::vector<bool> * vec,
                            size_t pos) {
                        mmDec = v2_mm128<T>::mullo(mmCol, mmInv);
                        mask_t maskGT = v2_mm128_cmp<T, std::greater>::cmp_mask(mmDec, mmDMax);
                        if (maskGT) {
                            // TODO we need a different bit vector implementation where we can store whole masks and not only single boolean values!
                            decltype(maskGT) test = 1;
                            for (size_t k = 0; k < steps; ++k, test <<= 1) {
                                if (maskGT & test) {
                                    (*vec)[pos + k] = true;
                                }
                            }
                        }
                        return maskGT;
                    }

                    static inline mask_t detect(
                            __m128i & mmDec,
                            __m128i mmCol,
                            __m128i mmInv,
                            __m128i mmDMax,
                            AN_indicator_vector * vec,
                            size_t pos,
                            resoid_t Aoid) {
                        mmDec = v2_mm128<T>::mullo(mmCol, mmInv);
                        mask_t maskGT = v2_mm128_cmp<T, std::greater>::cmp_mask(mmDec, mmDMax);
                        if (maskGT) {
                            decltype(maskGT) test = 1;
                            for (size_t k = 0; k < steps; ++k, test <<= 1) {
                                if (maskGT & test) {
                                    vec->push_back((pos + k) * Aoid);
                                }
                            }
                        }
                        return maskGT;
                    }

                };

            }
        }
    }
}

#endif /* SSEAN_HPP */
