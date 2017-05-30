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
 * File:   hashjoin.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 10-04-2017 22:02
 */

#include "hashjoin.tcc"

namespace ahead {
    namespace bat {
        namespace ops {

#define V2_HASHJOIN_SUB(V2TYPE_HEAD, V2TYPE_TAIL) \
template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * hashjoin(BAT<V2TYPE_HEAD, v2_void_t> * arg1, BAT<v2_void_t, V2TYPE_TAIL> * arg2, hash_side_t side); \
template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * hashjoin(BAT<V2TYPE_HEAD, v2_void_t> * arg1, BAT<v2_oid_t, V2TYPE_TAIL> * arg2, hash_side_t side); \
template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * hashjoin(BAT<V2TYPE_HEAD, v2_oid_t> * arg1, BAT<v2_void_t, V2TYPE_TAIL> * arg2, hash_side_t side); \
template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * hashjoin(BAT<V2TYPE_HEAD, v2_oid_t> * arg1, BAT<v2_oid_t, V2TYPE_TAIL> * arg2, hash_side_t side); \
template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * hashjoin(BAT<V2TYPE_HEAD, v2_id_t> * arg1, BAT<v2_id_t, V2TYPE_TAIL> * arg2, hash_side_t side); \
template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * hashjoin(BAT<V2TYPE_HEAD, v2_size_t> * arg1, BAT<v2_size_t, V2TYPE_TAIL> * arg2, hash_side_t side); \
template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * hashjoin(BAT<V2TYPE_HEAD, v2_tinyint_t> * arg1, BAT<v2_tinyint_t, V2TYPE_TAIL> * arg2, hash_side_t side); \
template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * hashjoin(BAT<V2TYPE_HEAD, v2_shortint_t> * arg1, BAT<v2_shortint_t, V2TYPE_TAIL> * arg2, hash_side_t side); \
template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * hashjoin(BAT<V2TYPE_HEAD, v2_int_t> * arg1, BAT<v2_int_t, V2TYPE_TAIL> * arg2, hash_side_t side); \
template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * hashjoin(BAT<V2TYPE_HEAD, v2_bigint_t> * arg1, BAT<v2_bigint_t, V2TYPE_TAIL> * arg2, hash_side_t side); \
template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * hashjoin(BAT<V2TYPE_HEAD, v2_str_t> * arg1, BAT<v2_str_t, V2TYPE_TAIL> * arg2, hash_side_t side); \
template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * hashjoin(BAT<V2TYPE_HEAD, v2_restiny_t> * arg1, BAT<v2_restiny_t, V2TYPE_TAIL> * arg2, hash_side_t side); \
template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * hashjoin(BAT<V2TYPE_HEAD, v2_resshort_t> * arg1, BAT<v2_resshort_t, V2TYPE_TAIL> * arg2, hash_side_t side); \
template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * hashjoin(BAT<V2TYPE_HEAD, v2_resint_t> * arg1, BAT<v2_resint_t, V2TYPE_TAIL> * arg2, hash_side_t side); \
template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * hashjoin(BAT<V2TYPE_HEAD, v2_resbigint_t> * arg1, BAT<v2_resbigint_t, V2TYPE_TAIL> * arg2, hash_side_t side); \
template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * hashjoin(BAT<V2TYPE_HEAD, v2_resstr_t> * arg1, BAT<v2_resstr_t, V2TYPE_TAIL> * arg2, hash_side_t side);

#define V2_HASHJOIN(V2TYPE) \
V2_HASHJOIN_SUB(v2_void_t, V2TYPE) \
V2_HASHJOIN_SUB(v2_oid_t, V2TYPE) \
V2_HASHJOIN_SUB(v2_id_t, V2TYPE) \
V2_HASHJOIN_SUB(v2_size_t, V2TYPE) \
V2_HASHJOIN_SUB(v2_tinyint_t, V2TYPE) \
V2_HASHJOIN_SUB(v2_shortint_t, V2TYPE) \
V2_HASHJOIN_SUB(v2_int_t, V2TYPE) \
V2_HASHJOIN_SUB(v2_bigint_t, V2TYPE) \
V2_HASHJOIN_SUB(v2_str_t, V2TYPE) \
V2_HASHJOIN_SUB(v2_restiny_t, V2TYPE) \
V2_HASHJOIN_SUB(v2_resshort_t, V2TYPE) \
V2_HASHJOIN_SUB(v2_resint_t, V2TYPE) \
V2_HASHJOIN_SUB(v2_resbigint_t, V2TYPE) \
V2_HASHJOIN_SUB(v2_resstr_t, V2TYPE)

        V2_HASHJOIN(v2_void_t)
        V2_HASHJOIN(v2_oid_t)
        V2_HASHJOIN(v2_id_t)
        V2_HASHJOIN(v2_size_t)
        V2_HASHJOIN(v2_tinyint_t)
        V2_HASHJOIN(v2_shortint_t)
        V2_HASHJOIN(v2_int_t)
        V2_HASHJOIN(v2_bigint_t)
        V2_HASHJOIN(v2_str_t)
        V2_HASHJOIN(v2_restiny_t)
        V2_HASHJOIN(v2_resshort_t)
        V2_HASHJOIN(v2_resint_t)
        V2_HASHJOIN(v2_resbigint_t)
        // V2_HASHJOIN(v2_resstr_t)

#undef V2_HASHJOIN
#undef V2_HASHJOIN_SUB

        }
    }
}
