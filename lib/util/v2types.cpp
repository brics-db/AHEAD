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
 * File:   v2types.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 19. November 2016, 23:46
 */

#include <limits>

#include <util/v2types.hpp>

const typename v2_tinyint_t::type_t v2_tinyint_t::dhm_emptykey = static_cast<typename v2_tinyint_t::type_t>(-1);
const typename v2_tinyint_t::type_t v2_tinyint_t::dhm_deletedkey = static_cast<typename v2_tinyint_t::type_t>(-2);

const typename v2_shortint_t::type_t v2_shortint_t::dhm_emptykey = static_cast<typename v2_shortint_t::type_t>(-1);
const typename v2_shortint_t::type_t v2_shortint_t::dhm_deletedkey = static_cast<typename v2_shortint_t::type_t>(-2);

const typename v2_int_t::type_t v2_int_t::dhm_emptykey = static_cast<typename v2_int_t::type_t>(-1);
const typename v2_int_t::type_t v2_int_t::dhm_deletedkey = static_cast<typename v2_int_t::type_t>(-2);

const typename v2_bigint_t::type_t v2_bigint_t::dhm_emptykey = static_cast<typename v2_bigint_t::type_t>(-1);
const typename v2_bigint_t::type_t v2_bigint_t::dhm_deletedkey = static_cast<typename v2_bigint_t::type_t>(-2);

const typename v2_char_t::type_t v2_char_t::dhm_emptykey = static_cast<typename v2_char_t::type_t>(-1);
const typename v2_char_t::type_t v2_char_t::dhm_deletedkey = static_cast<typename v2_char_t::type_t>(-2);

const typename v2_str_t::type_t v2_str_t::dhm_emptykey = nullptr;
const typename v2_str_t::type_t v2_str_t::dhm_deletedkey = reinterpret_cast<typename v2_str_t::type_t>(-1);

const typename v2_fixed_t::type_t v2_fixed_t::dhm_emptykey = std::numeric_limits<double>::quiet_NaN ();
const typename v2_fixed_t::type_t v2_fixed_t::dhm_deletedkey = std::numeric_limits<double>::infinity ();

const typename v2_id_t::type_t v2_id_t::dhm_emptykey = static_cast<typename v2_id_t::type_t>(-1);
const typename v2_id_t::type_t v2_id_t::dhm_deletedkey = static_cast<typename v2_id_t::type_t>(-2);

const typename v2_oid_t::type_t v2_oid_t::dhm_emptykey = static_cast<typename v2_oid_t::type_t>(-1);
const typename v2_oid_t::type_t v2_oid_t::dhm_deletedkey = static_cast<typename v2_oid_t::type_t>(-2);

const typename v2_void_t::type_t v2_void_t::dhm_emptykey = static_cast<typename v2_void_t::type_t>(-1);
const typename v2_void_t::type_t v2_void_t::dhm_deletedkey = static_cast<typename v2_void_t::type_t>(-2);

const typename v2_version_t::type_t v2_version_t::dhm_emptykey = static_cast<typename v2_version_t::type_t>(-1);
const typename v2_version_t::type_t v2_version_t::dhm_deletedkey = static_cast<typename v2_version_t::type_t>(-2);

const typename v2_size_t::type_t v2_size_t::dhm_emptykey = static_cast<typename v2_size_t::type_t>(-1);
const typename v2_size_t::type_t v2_size_t::dhm_deletedkey = static_cast<typename v2_size_t::type_t>(-2);

const typename v2_id_t::type_t ID_INVALID = static_cast<typename v2_id_t::type_t>(-1);
const typename v2_oid_t::type_t OID_INVALID = static_cast<typename v2_oid_t::type_t>(-1);
const typename v2_version_t::type_t VERSION_INVALID = static_cast<typename v2_version_t::type_t>(-1);