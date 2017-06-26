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
 * File:   SSECMP.hpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 24. Februar 2017, 14:07
 */

#ifndef SSECMP_HPP
#define SSECMP_HPP

#include <immintrin.h>

#include "SSE.hpp"
#include <column_operators/functors.hpp>

namespace ahead {
    namespace bat {
        namespace ops {
            namespace sse {

                template<typename T, template<typename > class Op>
                struct v2_mm128_cmp;

            }
        }
    }
}

#include "SSECMP_uint8.tcc"
#include "SSECMP_uint16.tcc"
#include "SSECMP_uint32.tcc"
#include "SSECMP_uint64.tcc"

#endif /* SSECMP_HPP */
