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
 * File:   groupby.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 10-04-2017 22:02
 */

#include "groupby.tcc"

namespace ahead {
    namespace bat {
        namespace ops {

#define V2_GROUPBY_SUB(Head, Tail) \
template std::pair<BAT<v2_void_t, v2_oid_t> *, BAT<v2_void_t, v2_oid_t> *> groupby<Head, Tail>(BAT<Head, Tail> * bat); \
template std::pair<BAT<v2_void_t, v2_oid_t> *, BAT<v2_void_t, v2_oid_t> *> groupby<Head, Tail>(BAT<Head, Tail> * bat, BAT<v2_void_t, v2_oid_t> * grouping, size_t numGroups);

#define V2_GROUPBY(Tail) \
V2_GROUPBY_SUB(v2_void_t, Tail) \
V2_GROUPBY_SUB(v2_oid_t, Tail)

            // V2_GROUPBY(v2_void_t)
            V2_GROUPBY(v2_oid_t)
            // V2_GROUPBY(v2_id_t)
            // V2_GROUPBY(v2_size_t)
            V2_GROUPBY(v2_tinyint_t)
            V2_GROUPBY(v2_shortint_t)
            V2_GROUPBY(v2_int_t)
            // V2_GROUPBY(v2_bigint_t)
            V2_GROUPBY(v2_str_t)
            V2_GROUPBY(v2_resoid_t)
            V2_GROUPBY(v2_restiny_t)
            V2_GROUPBY(v2_resshort_t)
            V2_GROUPBY(v2_resint_t)
        // V2_GROUPBY(v2_resbigint_t)
        // V2_GROUPBY(v2_resstr_t)

        }
    }
}
