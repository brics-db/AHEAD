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

#define V2_MATCHJOIN_SUB(V2HEAD, V2TAIL) \
template BAT<typename V2HEAD::v2_select_t, typename V2TAIL::v2_select_t> * matchjoin(BAT<V2HEAD, v2_void_t> * arg1, BAT<v2_void_t, V2TAIL> * arg2); \
template BAT<typename V2HEAD::v2_select_t, typename V2TAIL::v2_select_t> * matchjoin(BAT<V2HEAD, v2_void_t> * arg1, BAT<v2_oid_t, V2TAIL> * arg2); \
template BAT<typename V2HEAD::v2_select_t, typename V2TAIL::v2_select_t> * matchjoin(BAT<V2HEAD, v2_oid_t> * arg1, BAT<v2_oid_t, V2TAIL> * arg2); \
template BAT<typename V2HEAD::v2_select_t, typename V2TAIL::v2_select_t> * matchjoin(BAT<V2HEAD, v2_oid_t> * arg1, BAT<v2_void_t, V2TAIL> * arg2);

#define V2_MATCHJOIN(V2TAIL) \
V2_MATCHJOIN_SUB(v2_void_t, V2TAIL) \
V2_MATCHJOIN_SUB(v2_oid_t, V2TAIL) \
V2_MATCHJOIN_SUB(v2_id_t, V2TAIL) \
V2_MATCHJOIN_SUB(v2_size_t, V2TAIL) \
V2_MATCHJOIN_SUB(v2_tinyint_t, V2TAIL) \
V2_MATCHJOIN_SUB(v2_shortint_t, V2TAIL) \
V2_MATCHJOIN_SUB(v2_int_t, V2TAIL) \
V2_MATCHJOIN_SUB(v2_bigint_t, V2TAIL) \
V2_MATCHJOIN_SUB(v2_str_t, V2TAIL) \
V2_MATCHJOIN_SUB(v2_restiny_t, V2TAIL) \
V2_MATCHJOIN_SUB(v2_resshort_t, V2TAIL) \
V2_MATCHJOIN_SUB(v2_resint_t, V2TAIL) \
V2_MATCHJOIN_SUB(v2_resbigint_t, V2TAIL) \
V2_MATCHJOIN_SUB(v2_resstr_t, V2TAIL)

        }
    }
}

#endif /* LIB_COLUMN_OPERATORS_NORMAL_MATCHJOIN_HPP_ */
