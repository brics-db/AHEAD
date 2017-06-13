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
 * File:   matchjoin.hpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 01-06-2017 09:58
 */
#ifndef LIB_COLUMN_OPERATORS_NORMAL_MATCHJOIN_HPP_
#define LIB_COLUMN_OPERATORS_NORMAL_MATCHJOIN_HPP_

#include "matchjoin.tcc"

namespace ahead {
    namespace bat {
        namespace ops {

#define V2_MATCHJOIN_SUB(Head, Tail) \
template BAT<typename Head::v2_select_t, typename Tail::v2_select_t> * matchjoin(BAT<Head, v2_void_t> *, BAT<v2_void_t, Tail> *); \
template BAT<typename Head::v2_select_t, typename Tail::v2_select_t> * matchjoin(BAT<Head, v2_void_t> *, BAT<v2_oid_t, Tail> *); \
template BAT<typename Head::v2_select_t, typename Tail::v2_select_t> * matchjoin(BAT<Head, v2_oid_t> *, BAT<v2_oid_t, Tail> *); \
template BAT<typename Head::v2_select_t, typename Tail::v2_select_t> * matchjoin(BAT<Head, v2_oid_t> *, BAT<v2_void_t, Tail> *);

#define V2_MATCHJOIN(Tail) \
V2_MATCHJOIN_SUB(v2_void_t, Tail) \
V2_MATCHJOIN_SUB(v2_oid_t, Tail) \
V2_MATCHJOIN_SUB(v2_id_t, Tail) \
V2_MATCHJOIN_SUB(v2_size_t, Tail) \
V2_MATCHJOIN_SUB(v2_tinyint_t, Tail) \
V2_MATCHJOIN_SUB(v2_shortint_t, Tail) \
V2_MATCHJOIN_SUB(v2_int_t, Tail) \
V2_MATCHJOIN_SUB(v2_bigint_t, Tail) \
V2_MATCHJOIN_SUB(v2_str_t, Tail) \
V2_MATCHJOIN_SUB(v2_restiny_t, Tail) \
V2_MATCHJOIN_SUB(v2_resshort_t, Tail) \
V2_MATCHJOIN_SUB(v2_resint_t, Tail) \
V2_MATCHJOIN_SUB(v2_resbigint_t, Tail) \
V2_MATCHJOIN_SUB(v2_resstr_t, Tail) \
template BAT<v2_void_t, Tail> * fetchjoin(BAT<v2_void_t, v2_oid_t> *, BAT<v2_void_t, Tail> *);

        }
    }
}

#endif /* LIB_COLUMN_OPERATORS_NORMAL_MATCHJOIN_HPP_ */
