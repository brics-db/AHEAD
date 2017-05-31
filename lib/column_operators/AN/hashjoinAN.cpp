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
 * File:   hashjoinAN.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 10-04-2017 22:14
 */

#include <column_operators/ANbase.hpp>
#include "hashjoinAN.tcc"

namespace ahead {
    namespace bat {
        namespace ops {

#define V2_HASHJOIN_IMPL0(V2H1, V2T1, V2H2, V2T2) \
template std::tuple<TempBAT<typename V2H1::v2_select_t, typename V2T2::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> \
hashjoinAN(BAT<V2H1, V2T1> *, BAT<V2H2, V2T2> *, hash_side_t);

#define V2_HASHJOIN_IMPL1(V2H1, V2T1, V2H2, V2T2) \
template std::tuple<TempBAT<typename TypeMap<V2H1>::v2_encoded_t::v2_select_t, typename V2T2::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> \
hashjoinAN(BAT<V2H1, V2T1> *, BAT<V2H2, V2T2> *, typename TypeMap<V2H1>::v2_encoded_t::type_t, typename TypeMap<V2H1>::v2_encoded_t::type_t, hash_side_t);

#define V2_HASHJOIN_IMPL2(V2H1, V2T1, V2H2, V2T2) \
template std::tuple<TempBAT<typename TypeMap<V2H1>::v2_encoded_t::v2_select_t, typename TypeMap<V2T2>::v2_encoded_t::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> \
hashjoinAN(BAT<V2H1, V2T1> *, BAT<V2H2, V2T2> *, typename TypeMap<V2H1>::v2_encoded_t::type_t, typename TypeMap<V2H1>::v2_encoded_t::type_t, typename TypeMap<V2T2>::v2_encoded_t::type_t, typename TypeMap<V2T2>::v2_encoded_t::type_t, hash_side_t);

#define V2_HASHJOIN_SUB_AN0(V2HEAD, V2TAIL) \
V2_HASHJOIN_IMPL0(V2HEAD, v2_void_t, v2_void_t, V2TAIL) \
V2_HASHJOIN_IMPL0(V2HEAD, v2_void_t, v2_oid_t, V2TAIL) \
V2_HASHJOIN_IMPL0(V2HEAD, v2_oid_t, v2_void_t, V2TAIL) \
V2_HASHJOIN_IMPL0(V2HEAD, v2_oid_t, v2_oid_t, V2TAIL) \
V2_HASHJOIN_IMPL0(V2HEAD, v2_tinyint_t, v2_tinyint_t, V2TAIL) \
V2_HASHJOIN_IMPL0(V2HEAD, v2_shortint_t, v2_shortint_t, V2TAIL) \
V2_HASHJOIN_IMPL0(V2HEAD, v2_int_t, v2_int_t, V2TAIL) \
V2_HASHJOIN_IMPL0(V2HEAD, v2_bigint_t, v2_bigint_t, V2TAIL) \
V2_HASHJOIN_IMPL0(V2HEAD, v2_void_t, v2_resoid_t, V2TAIL) \
V2_HASHJOIN_IMPL0(V2HEAD, v2_resoid_t, v2_void_t, V2TAIL) \
V2_HASHJOIN_IMPL0(V2HEAD, v2_resoid_t, v2_oid_t, V2TAIL) \
V2_HASHJOIN_IMPL0(V2HEAD, v2_oid_t, v2_resoid_t, V2TAIL) \
V2_HASHJOIN_IMPL0(V2HEAD, v2_resoid_t, v2_resoid_t, V2TAIL) \
V2_HASHJOIN_IMPL0(V2HEAD, v2_restiny_t, v2_restiny_t, V2TAIL) \
V2_HASHJOIN_IMPL0(V2HEAD, v2_resshort_t, v2_resshort_t, V2TAIL) \
V2_HASHJOIN_IMPL0(V2HEAD, v2_resint_t, v2_resint_t, V2TAIL) \
V2_HASHJOIN_IMPL0(V2HEAD, v2_resbigint_t, v2_resbigint_t, V2TAIL)

#define V2_HASHJOIN_SUB_AN1(V2HEAD, V2TAIL) \
V2_HASHJOIN_IMPL1(V2HEAD, v2_void_t, v2_void_t, V2TAIL) \
V2_HASHJOIN_IMPL1(V2HEAD, v2_void_t, v2_oid_t, V2TAIL) \
V2_HASHJOIN_IMPL1(V2HEAD, v2_oid_t, v2_void_t, V2TAIL) \
V2_HASHJOIN_IMPL1(V2HEAD, v2_oid_t, v2_oid_t, V2TAIL) \
V2_HASHJOIN_IMPL1(V2HEAD, v2_tinyint_t, v2_tinyint_t, V2TAIL) \
V2_HASHJOIN_IMPL1(V2HEAD, v2_shortint_t, v2_shortint_t, V2TAIL) \
V2_HASHJOIN_IMPL1(V2HEAD, v2_int_t, v2_int_t, V2TAIL) \
V2_HASHJOIN_IMPL1(V2HEAD, v2_bigint_t, v2_bigint_t, V2TAIL) \
V2_HASHJOIN_IMPL1(V2HEAD, v2_void_t, v2_resoid_t, V2TAIL) \
V2_HASHJOIN_IMPL1(V2HEAD, v2_resoid_t, v2_void_t, V2TAIL) \
V2_HASHJOIN_IMPL1(V2HEAD, v2_resoid_t, v2_oid_t, V2TAIL) \
V2_HASHJOIN_IMPL1(V2HEAD, v2_oid_t, v2_resoid_t, V2TAIL) \
V2_HASHJOIN_IMPL1(V2HEAD, v2_resoid_t, v2_resoid_t, V2TAIL) \
V2_HASHJOIN_IMPL1(V2HEAD, v2_restiny_t, v2_restiny_t, V2TAIL) \
V2_HASHJOIN_IMPL1(V2HEAD, v2_resshort_t, v2_resshort_t, V2TAIL) \
V2_HASHJOIN_IMPL1(V2HEAD, v2_resint_t, v2_resint_t, V2TAIL) \
V2_HASHJOIN_IMPL1(V2HEAD, v2_resbigint_t, v2_resbigint_t, V2TAIL)

#define V2_HASHJOIN_SUB_AN2(V2HEAD, V2TAIL) \
V2_HASHJOIN_IMPL2(V2HEAD, v2_void_t, v2_void_t, V2TAIL) \
V2_HASHJOIN_IMPL2(V2HEAD, v2_void_t, v2_oid_t, V2TAIL) \
V2_HASHJOIN_IMPL2(V2HEAD, v2_oid_t, v2_void_t, V2TAIL) \
V2_HASHJOIN_IMPL2(V2HEAD, v2_oid_t, v2_oid_t, V2TAIL) \
V2_HASHJOIN_IMPL2(V2HEAD, v2_tinyint_t, v2_tinyint_t, V2TAIL) \
V2_HASHJOIN_IMPL2(V2HEAD, v2_shortint_t, v2_shortint_t, V2TAIL) \
V2_HASHJOIN_IMPL2(V2HEAD, v2_int_t, v2_int_t, V2TAIL) \
V2_HASHJOIN_IMPL2(V2HEAD, v2_bigint_t, v2_bigint_t, V2TAIL) \
V2_HASHJOIN_IMPL2(V2HEAD, v2_void_t, v2_resoid_t, V2TAIL) \
V2_HASHJOIN_IMPL2(V2HEAD, v2_resoid_t, v2_void_t, V2TAIL) \
V2_HASHJOIN_IMPL2(V2HEAD, v2_resoid_t, v2_oid_t, V2TAIL) \
V2_HASHJOIN_IMPL2(V2HEAD, v2_oid_t, v2_resoid_t, V2TAIL) \
V2_HASHJOIN_IMPL2(V2HEAD, v2_resoid_t, v2_resoid_t, V2TAIL) \
V2_HASHJOIN_IMPL2(V2HEAD, v2_restiny_t, v2_restiny_t, V2TAIL) \
V2_HASHJOIN_IMPL2(V2HEAD, v2_resshort_t, v2_resshort_t, V2TAIL) \
V2_HASHJOIN_IMPL2(V2HEAD, v2_resint_t, v2_resint_t, V2TAIL) \
V2_HASHJOIN_IMPL2(V2HEAD, v2_resbigint_t, v2_resbigint_t, V2TAIL)

#define V2_HASHJOIN_AN(V2TAIL) \
V2_HASHJOIN_SUB_AN0(v2_void_t, V2TAIL) \
V2_HASHJOIN_SUB_AN1(v2_void_t, V2TAIL) \
V2_HASHJOIN_SUB_AN2(v2_void_t, V2TAIL) \
V2_HASHJOIN_SUB_AN0(v2_oid_t, V2TAIL) \
V2_HASHJOIN_SUB_AN1(v2_oid_t, V2TAIL) \
V2_HASHJOIN_SUB_AN2(v2_oid_t, V2TAIL) \
V2_HASHJOIN_SUB_AN0(v2_resoid_t, V2TAIL) \
V2_HASHJOIN_SUB_AN1(v2_resoid_t, V2TAIL) \
V2_HASHJOIN_SUB_AN2(v2_resoid_t, V2TAIL)

        V2_HASHJOIN_AN(v2_void_t)
        V2_HASHJOIN_AN(v2_oid_t)
        V2_HASHJOIN_AN(v2_resoid_t)
        V2_HASHJOIN_AN(v2_restiny_t)
        V2_HASHJOIN_AN(v2_resshort_t)
        V2_HASHJOIN_AN(v2_str_t)

        }
    }
}
