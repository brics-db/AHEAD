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
 * File:   arithmeticAN.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 22. June 2017, 12:51
 */

#include <column_operators/functors.hpp>
#include "arithmeticAN_scalar.tcc"
#include "arithmeticAN_SSE.tcc"

namespace ahead {
    namespace bat {
        namespace ops {

#define ARITHMETIC_SUB(Result, Head1, Tail1, Head2, Tail2) \
template std::tuple<BAT<v2_void_t, Result> *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *> \
    arithmeticAN<ADD, Result>(BAT<Head1, Tail1> * bat1, BAT<Head2, Tail2> * bat2, typename Result::type_t AResult, typename Result::type_t AResultInv, resoid_t AOID); \
template std::tuple<BAT<v2_void_t, Result> *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *> \
    arithmeticAN<SUB, Result>(BAT<Head1, Tail1> * bat1, BAT<Head2, Tail2> * bat2, typename Result::type_t AResult, typename Result::type_t AResultInv, resoid_t AOID); \
template std::tuple<BAT<v2_void_t, Result> *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *> \
    arithmeticAN<MUL, Result>(BAT<Head1, Tail1> * bat1, BAT<Head2, Tail2> * bat2, typename Result::type_t AResult, typename Result::type_t AResultInv, resoid_t AOID); \
template std::tuple<BAT<v2_void_t, Result> *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *> \
    arithmeticAN<DIV, Result>(BAT<Head1, Tail1> * bat1, BAT<Head2, Tail2> * bat2, typename Result::type_t AResult, typename Result::type_t AResultInv, resoid_t AOID);

#define ARITHMETIC(Result, Tail1, Tail2) \
            namespace scalar { \
                ARITHMETIC_SUB(Result, v2_void_t, Tail1, v2_void_t, Tail2) \
            } \
            namespace sse { \
                ARITHMETIC_SUB(Result, v2_void_t, Tail1, v2_void_t, Tail2) \
            }

            ARITHMETIC(v2_resint_t, v2_resint_t, v2_resint_t)
            ARITHMETIC(v2_resbigint_t, v2_resint_t, v2_resint_t)

        }
    }
}
