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
 * File:   TempStorage.hpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 04-04-2017 14:48
 */
#ifndef COLUMN_STORAGE_TEMPSTORAGE_HPP_
#define COLUMN_STORAGE_TEMPSTORAGE_HPP_

#include <ColumnStore.h>
#include <column_storage/Bat.h>
#include <column_storage/TempBat.h>

namespace ahead {

#define DEFINE_BATS(v2_type_t)                              \
extern template class TempBAT<v2_type_t, v2_void_t>;        \
extern template class TempBAT<v2_type_t, v2_oid_t>;         \
extern template class TempBAT<v2_type_t, v2_tinyint_t>;     \
extern template class TempBAT<v2_type_t, v2_shortint_t>;    \
extern template class TempBAT<v2_type_t, v2_int_t>;         \
extern template class TempBAT<v2_type_t, v2_bigint_t>;      \
extern template class TempBAT<v2_type_t, v2_char_t>;        \
extern template class TempBAT<v2_type_t, v2_str_t>;         \
extern template class TempBAT<v2_type_t, v2_fixed_t>;       \
extern template class TempBAT<v2_type_t, v2_id_t>;          \
extern template class TempBAT<v2_type_t, v2_size_t>;        \
extern template class TempBAT<v2_type_t, v2_resoid_t>;      \
extern template class TempBAT<v2_type_t, v2_restiny_t>;     \
extern template class TempBAT<v2_type_t, v2_resshort_t>;    \
extern template class TempBAT<v2_type_t, v2_resint_t>;      \
extern template class TempBAT<v2_type_t, v2_resbigint_t>;   \
extern template class TempBAT<v2_type_t, v2_resstr_t>;

    DEFINE_BATS(v2_void_t)
    DEFINE_BATS(v2_oid_t)
    DEFINE_BATS(v2_tinyint_t)
    DEFINE_BATS(v2_shortint_t)
    DEFINE_BATS(v2_int_t)
    DEFINE_BATS(v2_bigint_t)
    DEFINE_BATS(v2_char_t)
    DEFINE_BATS(v2_str_t)
    DEFINE_BATS(v2_fixed_t)
    DEFINE_BATS(v2_id_t)
    DEFINE_BATS(v2_size_t)
    DEFINE_BATS(v2_resoid_t)
    DEFINE_BATS(v2_restiny_t)
    DEFINE_BATS(v2_resshort_t)
    DEFINE_BATS(v2_resint_t)
    DEFINE_BATS(v2_resbigint_t)
    DEFINE_BATS(v2_resstr_t)

#undef DEFINE_BATS

}

#endif /* COLUMN_STORAGE_TEMPSTORAGE_HPP_ */
