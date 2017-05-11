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
 * File:   matchjoinAN.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 10-04-2017 22:14
 */

#include <column_operators/AN/matchjoinAN.tcc>

namespace ahead {
    namespace bat {
        namespace ops {

#define V2_MATCHJOIN_AN_SUB4(H1, T1, H2, T2) \
template std::tuple<TempBAT<typename H1::v2_select_t, typename T2::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> matchjoinAN(BAT<H1, T1> *arg1, BAT<H2, T2> *arg2); \
template std::tuple<TempBAT<typename H1::v2_select_t, typename T2::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> matchjoinAN(BAT<H1, T1> *arg1, BAT<H2, T2> *arg2, typename TypeMap<H1>::v2_encoded_t::type_t AH1reenc, typename TypeMap<H1>::v2_encoded_t::type_t AH1InvReenc); \
template std::tuple<TempBAT<typename H1::v2_select_t, typename T2::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> matchjoinAN(BAT<H1, T1> *arg1, BAT<H2, T2> *arg2, typename TypeMap<H1>::v2_encoded_t::type_t AH1reenc, typename TypeMap<H1>::v2_encoded_t::type_t AH1InvReenc, typename TypeMap<T1>::v2_encoded_t::type_t AT2Reenc, typename TypeMap<T1>::v2_encoded_t::type_t AT2InvReenc);

#define V2_MATCHJOIN_AN_SUB2(H1, T1) \
V2_MATCHJOIN_AN_SUB4(H1, T1, T1, v2_oid_t) \
V2_MATCHJOIN_AN_SUB4(H1, T1, T1, v2_tinyint_t) \
V2_MATCHJOIN_AN_SUB4(H1, T1, T1, v2_shortint_t) \
V2_MATCHJOIN_AN_SUB4(H1, T1, T1, v2_int_t) \
V2_MATCHJOIN_AN_SUB4(H1, T1, T1, v2_bigint_t) \
V2_MATCHJOIN_AN_SUB4(H1, T1, T1, v2_resoid_t) \
V2_MATCHJOIN_AN_SUB4(H1, T1, T1, v2_restinyint_t) \
V2_MATCHJOIN_AN_SUB4(H1, T1, T1, v2_resshortint_t) \
V2_MATCHJOIN_AN_SUB4(H1, T1, T1, v2_resint_t) \
V2_MATCHJOIN_AN_SUB4(H1, T1, T1, v2_resbigint_t)

#define V2_MATCHJOIN_AN_SUB1(H1) \
V2_MATCHJOIN_AN_SUB2(H1, v2_resoid_t) \
V2_MATCHJOIN_AN_SUB2(H1, v2_restinyint_t) \
V2_MATCHJOIN_AN_SUB2(H1, v2_resshortint_t) \
V2_MATCHJOIN_AN_SUB2(H1, v2_resint_t) \
V2_MATCHJOIN_AN_SUB2(H1, v2_resbigint_t)

        // V2_MATCHJOIN_AN_SUB1(v2_void_t)
        // V2_MATCHJOIN_AN_SUB1(v2_oid_t)
        // V2_MATCHJOIN_AN_SUB1(v2_tinyint_t)
        // V2_MATCHJOIN_AN_SUB1(v2_shortint_t)
        // V2_MATCHJOIN_AN_SUB1(v2_int_t)
        // V2_MATCHJOIN_AN_SUB1(v2_bigint_t)
        // V2_MATCHJOIN_AN_SUB1(v2_resoid_t)
        // V2_MATCHJOIN_AN_SUB1(v2_restinyint_t)
        // V2_MATCHJOIN_AN_SUB1(v2_resshortint_t)
        // V2_MATCHJOIN_AN_SUB1(v2_resint_t)
        // V2_MATCHJOIN_AN_SUB1(v2_resbigint_t)
            V2_MATCHJOIN_AN_SUB4(v2_resoid_t, v2_resoid_t, v2_resoid_t, v2_restiny_t)

#undef V2_MATCHJOIN_AN_SUB1
#undef V2_MATCHJOIN_AN_SUB2
#undef V2_MATCHJOIN_AN_SUB4

        }
    }
}
