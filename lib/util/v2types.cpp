// Copyright (c) 2016 Till Kolditz
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

/* 
 * File:   v2types.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 19. November 2016, 23:46
 */

#include <limits>

#include <util/v2types.hpp>

const typename v2_tinyint_t::type_t v2_tinyint_t::dhm_emptykey = static_cast<typename v2_tinyint_t::type_t> (-1);
const typename v2_tinyint_t::type_t v2_tinyint_t::dhm_deletedkey = static_cast<typename v2_tinyint_t::type_t> (-2);

const typename v2_shortint_t::type_t v2_shortint_t::dhm_emptykey = static_cast<typename v2_shortint_t::type_t> (-1);
const typename v2_shortint_t::type_t v2_shortint_t::dhm_deletedkey = static_cast<typename v2_shortint_t::type_t> (-2);

const typename v2_int_t::type_t v2_int_t::dhm_emptykey = static_cast<typename v2_int_t::type_t> (-1);
const typename v2_int_t::type_t v2_int_t::dhm_deletedkey = static_cast<typename v2_int_t::type_t> (-2);

const typename v2_bigint_t::type_t v2_bigint_t::dhm_emptykey = static_cast<typename v2_bigint_t::type_t> (-1);
const typename v2_bigint_t::type_t v2_bigint_t::dhm_deletedkey = static_cast<typename v2_bigint_t::type_t> (-2);

const typename v2_char_t::type_t v2_char_t::dhm_emptykey = static_cast<typename v2_char_t::type_t> (-1);
const typename v2_char_t::type_t v2_char_t::dhm_deletedkey = static_cast<typename v2_char_t::type_t> (-2);

const typename v2_str_t::type_t v2_str_t::dhm_emptykey = nullptr;
const typename v2_str_t::type_t v2_str_t::dhm_deletedkey = reinterpret_cast<typename v2_str_t::type_t> (-1);

const typename v2_fixed_t::type_t v2_fixed_t::dhm_emptykey = std::numeric_limits<double>::quiet_NaN();
const typename v2_fixed_t::type_t v2_fixed_t::dhm_deletedkey = std::numeric_limits<double>::infinity();

const typename v2_id_t::type_t v2_id_t::dhm_emptykey = static_cast<typename v2_id_t::type_t> (-1);
const typename v2_id_t::type_t v2_id_t::dhm_deletedkey = static_cast<typename v2_id_t::type_t> (-2);

const typename v2_oid_t::type_t v2_oid_t::dhm_emptykey = static_cast<typename v2_oid_t::type_t> (-1);
const typename v2_oid_t::type_t v2_oid_t::dhm_deletedkey = static_cast<typename v2_oid_t::type_t> (-2);

const typename v2_void_t::type_t v2_void_t::dhm_emptykey = static_cast<typename v2_void_t::type_t> (-1);
const typename v2_void_t::type_t v2_void_t::dhm_deletedkey = static_cast<typename v2_void_t::type_t> (-2);

const typename v2_version_t::type_t v2_version_t::dhm_emptykey = static_cast<typename v2_version_t::type_t> (-1);
const typename v2_version_t::type_t v2_version_t::dhm_deletedkey = static_cast<typename v2_version_t::type_t> (-2);

const typename v2_size_t::type_t v2_size_t::dhm_emptykey = static_cast<typename v2_size_t::type_t> (-1);
const typename v2_size_t::type_t v2_size_t::dhm_deletedkey = static_cast<typename v2_size_t::type_t> (-2);

const typename v2_id_t::type_t ID_INVALID = static_cast<typename v2_id_t::type_t> (-1);
const typename v2_oid_t::type_t OID_INVALID = static_cast<typename v2_oid_t::type_t> (-1);
const typename v2_version_t::type_t VERSION_INVALID = static_cast<typename v2_version_t::type_t> (-1);