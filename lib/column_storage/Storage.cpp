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

#include <column_storage/Storage.hpp>

namespace v2 {

    template class ColumnBAT<v2_tinyint_t> ;
    template class ColumnBAT<v2_shortint_t> ;
    template class ColumnBAT<v2_int_t> ;
    template class ColumnBAT<v2_bigint_t> ;
    template class ColumnBAT<v2_char_t> ;
    template class ColumnBAT<v2_str_t> ;
    template class ColumnBAT<v2_fixed_t> ;
    template class ColumnBAT<v2_id_t> ;
    template class ColumnBAT<v2_oid_t> ;

#define DEFINE_TEMP_BATS(v2_type_t)                         \
template class TempBAT<v2_type_t, v2_tinyint_t> ;    \
template class TempBAT<v2_type_t, v2_shortint_t> ;   \
template class TempBAT<v2_type_t, v2_int_t> ;        \
template class TempBAT<v2_type_t, v2_bigint_t> ;     \
template class TempBAT<v2_type_t, v2_char_t> ;       \
template class TempBAT<v2_type_t, v2_str_t> ;        \
template class TempBAT<v2_type_t, v2_fixed_t> ;      \
template class TempBAT<v2_type_t, v2_id_t> ;         \
template class TempBAT<v2_type_t, v2_size_t> ;       \
template class TempBAT<v2_type_t, v2_oid_t> ;        \
template class TempBAT<v2_type_t, v2_void_t> ;       \
template class TempBAT<v2_type_t, v2_restiny_t> ;    \
template class TempBAT<v2_type_t, v2_resshort_t> ;   \
template class TempBAT<v2_type_t, v2_resint_t> ;     \
template class TempBAT<v2_type_t, v2_resbigint_t> ;  \
template class TempBAT<v2_type_t, v2_resstr_t> ;     \

    DEFINE_TEMP_BATS(v2_tinyint_t)
    DEFINE_TEMP_BATS(v2_shortint_t)
    DEFINE_TEMP_BATS(v2_int_t)
    DEFINE_TEMP_BATS(v2_bigint_t)
    DEFINE_TEMP_BATS(v2_char_t)
    DEFINE_TEMP_BATS(v2_str_t)
    DEFINE_TEMP_BATS(v2_fixed_t)
    DEFINE_TEMP_BATS(v2_id_t)
    DEFINE_TEMP_BATS(v2_size_t)
    DEFINE_TEMP_BATS(v2_oid_t)
    DEFINE_TEMP_BATS(v2_void_t)
    DEFINE_TEMP_BATS(v2_restiny_t)
    DEFINE_TEMP_BATS(v2_resshort_t)
    DEFINE_TEMP_BATS(v2_resint_t)
    DEFINE_TEMP_BATS(v2_resbigint_t)
    DEFINE_TEMP_BATS(v2_resstr_t)

#undef DEFINE_TEMP_BATS

}
