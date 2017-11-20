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
 * File:   AVX512.hpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 09. August 2017, 14:19
 */

#pragma once

#define LIB_COLUMN_OPERATORS_SIMD_AVX512_HPP_

#if defined(AHEAD_AVX512) or ( defined(__AVX512CD__) and defined(__AVX512ER__) and defined(__AVX512F__) and defined(__AVX512PF__) )
#ifndef AHEAD_AVX512
#define AHEAD_AVX512
#endif /* AHEAD_AVX512 */
#endif

#include <cstdint>
#include <cstdlib>
#include <immintrin.h>

#include <column_operators/functors.hpp>

#include "SSE.hpp"

#include "AVX512_base.tcc"
#include "AVX512_uint08.tcc"
#include "AVX512_uint16.tcc"
#include "AVX512_uint32.tcc"
#include "AVX512_uint64.tcc"

namespace ahead {
    namespace bat {
        namespace ops {
            namespace simd {

                template<typename T>
                struct mm<__m512i, T> :
                        public avx512::mm512<T> {
                    typedef avx512::mm512<T> BASE;

                    using BASE::mask_t;

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

                    static inline __m512i loadu(
                            __m512i * src) {
                        return _mm512_loadu_si512(src);
                    }

                    static inline void storeu(
                            void * dst,
                            __m512i src) {
                        _mm512_storeu_si512(dst, src);
                    }
                };

            }
        }
    }
}
