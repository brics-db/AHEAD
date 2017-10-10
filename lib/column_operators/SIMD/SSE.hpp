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

#define LIB_COLUMN_OPERATORS_SIMD_SSE_HPP_

#include <algorithm>
#include <cstdint>
#include <immintrin.h>

#include <column_operators/functors.hpp>

#include "SIMD.hpp"

namespace ahead {
    namespace bat {
        namespace ops {
            namespace simd {
                namespace sse {

                    template<typename ...Types>
                    struct mm128;

                }
            }
        }
    }
}

#include "SSE_uint08.tcc"
#include "SSE_uint16.tcc"
#include "SSE_uint32.tcc"
#include "SSE_uint64.tcc"

namespace ahead {
    namespace bat {
        namespace ops {
            namespace simd {

                template<typename T>
                struct mm<__m128i, T> :
                        public sse::mm128<T> {

                    typedef sse::mm128<T> BASE;

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

                    static inline __m128i loadu(
                            __m128i * src) {
                        return _mm_lddqu_si128(src);
                    }

                    static inline void storeu(
                            __m128i * dst,
                            __m128i src) {
                        _mm_storeu_si128(dst, src);
                    }
                };

            }
        }
    }
}
