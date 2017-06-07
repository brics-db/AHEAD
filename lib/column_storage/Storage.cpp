// Copyright (c) 2016-2017 Till Kolditz
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
 * Storage.cpp
 *
 *  Created on: 31.03.2017
 *      Author: Till Kolditz <till.kolditz@gmail.com>
 */

#include <column_storage/TempStorage.hpp>
#include "ColumnBat.hpp"

namespace ahead {

#define COLUMNBAT(Tail)                                              \
template class ColumnBAT<Tail>;

    COLUMNBAT(v2_tinyint_t)
    COLUMNBAT(v2_shortint_t)
    COLUMNBAT(v2_int_t)
    COLUMNBAT(v2_bigint_t)
    COLUMNBAT(v2_char_t)
    COLUMNBAT(v2_str_t)
    COLUMNBAT(v2_fixed_t)
    COLUMNBAT(v2_id_t)
    COLUMNBAT(v2_oid_t)
    COLUMNBAT(v2_restiny_t)
    COLUMNBAT(v2_resshort_t)
    COLUMNBAT(v2_resint_t)
    COLUMNBAT(v2_resbigint_t)

#define TEMPBAT(V2Head) \
template class TempBAT<V2Head, v2_void_t> ; \
template class TempBAT<V2Head, v2_tinyint_t> ; \
template class TempBAT<V2Head, v2_shortint_t> ; \
template class TempBAT<V2Head, v2_int_t> ; \
template class TempBAT<V2Head, v2_bigint_t> ; \
template class TempBAT<V2Head, v2_char_t> ; \
template class TempBAT<V2Head, v2_str_t> ; \
template class TempBAT<V2Head, v2_fixed_t> ; \
template class TempBAT<V2Head, v2_id_t> ; \
template class TempBAT<V2Head, v2_size_t> ; \
template class TempBAT<V2Head, v2_oid_t> ; \
template class TempBAT<V2Head, v2_restiny_t> ; \
template class TempBAT<V2Head, v2_resshort_t> ; \
template class TempBAT<V2Head, v2_resint_t> ; \
template class TempBAT<V2Head, v2_resbigint_t> ; \
template class TempBAT<V2Head, v2_resstr_t> ;

    TEMPBAT(v2_void_t)
    TEMPBAT(v2_tinyint_t)
    TEMPBAT(v2_shortint_t)
    TEMPBAT(v2_int_t)
    TEMPBAT(v2_bigint_t)
    TEMPBAT(v2_char_t)
    TEMPBAT(v2_str_t)
    TEMPBAT(v2_fixed_t)
    TEMPBAT(v2_id_t)
    TEMPBAT(v2_size_t)
    TEMPBAT(v2_oid_t)
    TEMPBAT(v2_restiny_t)
    TEMPBAT(v2_resshort_t)
    TEMPBAT(v2_resint_t)
    TEMPBAT(v2_resbigint_t)
    TEMPBAT(v2_resstr_t)

}
