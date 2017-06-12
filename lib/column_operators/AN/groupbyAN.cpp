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

#define V2_GROUPBY_AN_SUB(Head, Tail) \
template std::tuple<BAT<v2_void_t, v2_resoid_t>*, BAT<v2_void_t, v2_resoid_t> *, AN_indicator_vector *, AN_indicator_vector *> groupbyAN(BAT<Head, Tail> * bat, resoid_t AOID, resoid_t AOIDinv); \
template std::tuple<BAT<v2_void_t, v2_resoid_t>*, BAT<v2_void_t, v2_resoid_t> *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *> groupbyAN(BAT<Head, Tail> * bat, BAT<v2_void_t, v2_resoid_t> * grouping, size_t numGroups, resoid_t AOID, resoid_t AOIDinv);

#define V2_GROUPBY_AN(Tail) \
V2_GROUPBY_AN_SUB(v2_void_t, Tail) \
V2_GROUPBY_AN_SUB(v2_oid_t, Tail)

            // V2_GROUPBY_AN(v2_void_t)
            V2_GROUPBY_AN(v2_oid_t)
            // V2_GROUPBY_AN(v2_id_t)
            // V2_GROUPBY_AN(v2_size_t)
            V2_GROUPBY_AN(v2_tinyint_t)
            V2_GROUPBY_AN(v2_shortint_t)
            V2_GROUPBY_AN(v2_int_t)
            // V2_GROUPBY_AN(v2_bigint_t)
            V2_GROUPBY_AN(v2_str_t)
            V2_GROUPBY_AN(v2_resoid_t)
            V2_GROUPBY_AN(v2_restiny_t)
            V2_GROUPBY_AN(v2_resshort_t)
            V2_GROUPBY_AN(v2_resint_t)
        // V2_GROUPBY_AN(v2_resbigint_t)
        // V2_GROUPBY_AN(v2_resstr_t)

        }
    }
}
