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
 * File:   arithmetic.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 14-06-2017 16:10
 */

#include <column_operators/functors.hpp>
#include "arithmetic_scalar.tcc"
#include "arithmetic_SSE.tcc"

namespace ahead {
    namespace bat {
        namespace ops {

#define ARITHMETIC_SUB(Result, Head1, Tail1, Head2, Tail2) \
template BAT<v2_void_t, Result> * arithmetic<ahead::add, Result>(BAT<Head1, Tail1> * bat1, BAT<Head2, Tail2> * bat2); \
template BAT<v2_void_t, Result> * arithmetic<ahead::sub, Result>(BAT<Head1, Tail1> * bat1, BAT<Head2, Tail2> * bat2); \
template BAT<v2_void_t, Result> * arithmetic<ahead::mul, Result>(BAT<Head1, Tail1> * bat1, BAT<Head2, Tail2> * bat2); \
template BAT<v2_void_t, Result> * arithmetic<ahead::div, Result>(BAT<Head1, Tail1> * bat1, BAT<Head2, Tail2> * bat2);

#define ARITHMETIC(Result, Tail1, Tail2) \
        namespace scalar { \
            ARITHMETIC_SUB(Result, v2_void_t, Tail1, v2_void_t, Tail2) \
        } \
        namespace simd { \
            namespace sse { \
                ARITHMETIC_SUB(Result, v2_void_t, Tail1, v2_void_t, Tail2) \
            } \
        }

            ARITHMETIC(v2_int_t, v2_int_t, v2_int_t)
            ARITHMETIC(v2_resint_t, v2_resint_t, v2_resint_t)

        }
    }
}
