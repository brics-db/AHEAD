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

#include <column_operators/SSE.hpp>
#include <column_operators/SSECMP.hpp>

namespace v2 {
    namespace bat {
        namespace ops {

            template<typename T>
            struct v2_mm128_AN {

                typedef T _Tp;
                constexpr static const size_t steps = sizeof(__m128i ) / sizeof(_Tp);

                static inline void detect(__m128i mmCol, __m128i mmInv, __m128i mmDMax, std::vector<bool> * vec, size_t pos) {
                    uint8_t maskGT = v2_mm128_cmp<_Tp, std::greater_equal>::cmp_mask(v2_mm128<_Tp>::mullo(mmCol, mmInv), mmDMax);
                    if (maskGT) {
                        uint8_t test = 1;
                        for (size_t k = 0; k < steps; ++k, test <<= 1) {
                            if (maskGT & test) {
                                (*vec)[pos + k] = true;
                            }
                        }
                    }
                }

                static inline void detect(__m128i mmDec, __m128i mmCol, __m128i mmInv, __m128i mmDMax, std::vector<bool> * vec, size_t pos) {
                    mmDec = v2_mm128<_Tp>::mullo(mmCol, mmInv);
                    uint8_t maskGT = v2_mm128_cmp<_Tp, std::greater_equal>::cmp_mask(mmDec, mmDMax);
                    if (maskGT) {
                        uint8_t test = 1;
                        for (size_t k = 0; k < steps; ++k, test <<= 1) {
                            if (maskGT & test) {
                                (*vec)[pos + k] = true;
                            }
                        }
                    }
                }

            };

        }
    }
}

#endif /* SSEAN_HPP */
