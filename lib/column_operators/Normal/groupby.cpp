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

#include <column_operators/Normal/groupby.tcc>

namespace ahead {
    namespace bat {
        namespace ops {

#define V2_GROUPBY(V2TYPE) \
template std::pair<TempBAT<v2_void_t, v2_oid_t>*, TempBAT<v2_void_t, V2TYPE>*> groupby(BAT<v2_void_t, V2TYPE>* bat); \
template std::pair<TempBAT<v2_oid_t, v2_oid_t>*, TempBAT<v2_void_t, V2TYPE>*> groupby(BAT<v2_oid_t, V2TYPE>* bat); \
template std::pair<TempBAT<v2_id_t, v2_oid_t>*, TempBAT<v2_void_t, V2TYPE>*> groupby(BAT<v2_id_t, V2TYPE>* bat); \
template std::pair<TempBAT<v2_size_t, v2_oid_t>*, TempBAT<v2_void_t, V2TYPE>*> groupby(BAT<v2_size_t, V2TYPE>* bat); \
template std::pair<TempBAT<v2_tinyint_t, v2_oid_t>*, TempBAT<v2_void_t, V2TYPE>*> groupby(BAT<v2_tinyint_t, V2TYPE>* bat); \
template std::pair<TempBAT<v2_shortint_t, v2_oid_t>*, TempBAT<v2_void_t, V2TYPE>*> groupby(BAT<v2_shortint_t, V2TYPE>* bat); \
template std::pair<TempBAT<v2_int_t, v2_oid_t>*, TempBAT<v2_void_t, V2TYPE>*> groupby(BAT<v2_int_t, V2TYPE>* bat); \
template std::pair<TempBAT<v2_bigint_t, v2_oid_t>*, TempBAT<v2_void_t, V2TYPE>*> groupby(BAT<v2_bigint_t, V2TYPE>* bat); \
template std::pair<TempBAT<v2_str_t, v2_oid_t>*, TempBAT<v2_void_t, V2TYPE>*> groupby(BAT<v2_str_t, V2TYPE>* bat); \
template std::pair<TempBAT<v2_resoid_t, v2_oid_t>*, TempBAT<v2_void_t, V2TYPE>*> groupby(BAT<v2_resoid_t, V2TYPE>* bat); \
template std::pair<TempBAT<v2_restiny_t, v2_oid_t>*, TempBAT<v2_void_t, V2TYPE>*> groupby(BAT<v2_restiny_t, V2TYPE>* bat); \
template std::pair<TempBAT<v2_resshort_t, v2_oid_t>*, TempBAT<v2_void_t, V2TYPE>*> groupby(BAT<v2_resshort_t, V2TYPE>* bat); \
template std::pair<TempBAT<v2_resint_t, v2_oid_t>*, TempBAT<v2_void_t, V2TYPE>*> groupby(BAT<v2_resint_t, V2TYPE>* bat); \
template std::pair<TempBAT<v2_resbigint_t, v2_oid_t>*, TempBAT<v2_void_t, V2TYPE>*> groupby(BAT<v2_resbigint_t, V2TYPE>* bat); \
template std::pair<TempBAT<v2_resstr_t, v2_oid_t>*, TempBAT<v2_void_t, V2TYPE>*> groupby(BAT<v2_resstr_t, V2TYPE>* bat);

            // V2_GROUPBY(v2_void_t)
            // V2_GROUPBY(v2_oid_t)
            // V2_GROUPBY(v2_id_t)
            // V2_GROUPBY(v2_size_t)
            // V2_GROUPBY(v2_tinyint_t)
            // V2_GROUPBY(v2_shortint_t)
            // V2_GROUPBY(v2_int_t)
            // V2_GROUPBY(v2_bigint_t)
            // V2_GROUPBY(v2_str_t)
            // V2_GROUPBY(v2_resoid_t)
            // V2_GROUPBY(v2_restiny_t)
            // V2_GROUPBY(v2_resshort_t)
            // V2_GROUPBY(v2_resint_t)
            // V2_GROUPBY(v2_resbigint_t)
            // V2_GROUPBY(v2_resstr_t)

#undef V2_GROUPBY

#define V2_GROUPEDSUM_SUB(V2RESULT, V2TAIL1, V2TAIL2, V2TAIL3) \
template std::tuple<TempBAT<v2_void_t, V2RESULT>*, TempBAT<v2_void_t, v2_oid_t>*, TempBAT<v2_void_t, V2TAIL2>*, TempBAT<v2_void_t, v2_oid_t>*, TempBAT<v2_void_t, V2TAIL3>*> groupedSum(BAT<v2_oid_t, V2TAIL1>* bat1, BAT<v2_oid_t, V2TAIL2>* bat2, BAT<v2_oid_t, V2TAIL3>* bat3);

            V2_GROUPEDSUM_SUB(v2_bigint_t, v2_int_t, v2_shortint_t, v2_str_t)

#undef V2_GROUPBY

        }
    }
}
