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
 * File:   groupbyAN.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 10-04-2017 22:14
 */

#include <column_operators/ANbase.hpp>
#include "groupbyAN.tcc"

namespace ahead {
    namespace bat {
        namespace ops {

#define GROUPBY_AN(V2Head, V2Tail) \
template std::tuple<BAT<V2Head, v2_resoid_t>*, BAT<v2_void_t, V2Tail>*, std::vector<bool>*, std::vector<bool>*> \
groupbyAN( \
        BAT<V2Head, V2Tail> *, \
        typename v2_resoid_t::type_t, \
        typename v2_resoid_t::type_t \
        );

#define GROUPBY_AN_REENC(V2Head, V2Tail) \
template std::tuple<BAT<V2Head, v2_resoid_t>*, BAT<v2_void_t, V2Tail>*, std::vector<bool>*, std::vector<bool>*> \
groupbyAN( \
        BAT<V2Head, V2Tail> *, \
        typename V2Head::type_t, \
        typename V2Head::type_t, \
        typename V2Tail::type_t, \
        typename V2Tail::type_t, \
        typename v2_resoid_t::type_t, \
        typename v2_resoid_t::type_t \
        );

#define GROUPEDSUM_AN(V2Result, V2Head1, V2Tail1, V2Head2, V2Tail2, V2Head3, V2Tail3) \
template std::tuple<BAT<v2_void_t, V2Result>*, BAT<v2_void_t, v2_resoid_t>*, BAT<v2_void_t, V2Tail2>*, BAT<v2_void_t, v2_resoid_t>*, BAT<v2_void_t, V2Tail3>*, \
    std::vector<bool>*, std::vector<bool>*,  std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> \
groupedSumAN( \
        BAT<V2Head1, V2Tail1> *, \
        BAT<V2Head2, V2Tail2> *, \
        BAT<V2Head3, V2Tail3> *, \
        typename v2_resoid_t::type_t, \
        typename v2_resoid_t::type_t, \
        typename V2Result::type_t ARes, \
        typename v2_resoid_t::type_t \
        );

#define GROUPEDSUM_AN_REENC(V2Result, V2Head1, V2Tail1, V2Head2, V2Tail2, V2Head3, V2Tail3) \
template std::tuple<BAT<v2_void_t, V2Result>*, BAT<v2_void_t, v2_resoid_t>*, BAT<v2_void_t, V2Tail2>*, BAT<v2_void_t, v2_resoid_t>*, BAT<v2_void_t, V2Tail3>*, \
    std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> \
groupedSumAN( \
        BAT<V2Head1, V2Tail1> *, \
        BAT<V2Head2, V2Tail2> *, \
        BAT<V2Head3, V2Tail3> *, \
        typename V2Head2::type_t, \
        typename V2Head2::type_t, \
        typename V2Tail2::type_t, \
        typename V2Tail2::type_t, \
        typename V2Head3::type_t, \
        typename V2Head3::type_t, \
        typename V2Tail3::type_t, \
        typename V2Tail3::type_t, \
        typename v2_resoid_t::type_t, \
        typename v2_resoid_t::type_t, \
        typename V2Result::type_t ARes, \
        typename v2_resoid_t::type_t \
        );

            GROUPEDSUM_AN(v2_resbigint_t, v2_resoid_t, v2_resint_t, v2_resoid_t, v2_resshort_t, v2_resoid_t, v2_str_t)
            GROUPEDSUM_AN_REENC(v2_resbigint_t, v2_resoid_t, v2_resint_t, v2_resoid_t, v2_resshort_t, v2_resoid_t, v2_str_t)

        }
    }
}
