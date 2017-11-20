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

#define LIB_COLUMN_OPERATORS_SIMD_AVX2_HPP_

#include <cstdint>
#include <cstdlib>
#include <immintrin.h>

#include <column_operators/functors.hpp>

#include "SSE.hpp"

#include "AVX2_base.tcc"
#include "AVX2_uint08.tcc"
#include "AVX2_uint16.tcc"
#include "AVX2_uint32.tcc"
#include "AVX2_uint64.tcc"

namespace ahead {
    namespace bat {
        namespace ops {
            namespace simd {

                template<typename T>
                struct mm<__m256i, T> :
                        public avx2::mm256<T> {

                    typedef avx2::mm256<T> BASE;

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

                    static inline __m256i loadu(
                            __m256i * src) {
                        return _mm256_lddqu_si256(src);
                    }

                    static inline void storeu(
                            __m256i * dst,
                            __m256i src) {
                        _mm256_storeu_si256(dst, src);
                    }
                };

            }
        }
    }
}
