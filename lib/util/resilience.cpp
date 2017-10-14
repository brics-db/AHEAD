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
 * File:   resilience.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 29. Juli 2016, 12:09
 */

#include <limits>

#include <util/resilience.hpp>

namespace ahead {

    const constexpr std::array<A_t, 16> ANParameters::Atiny;
    const constexpr std::array<restiny_t, 16> ANParameters::AtinyInv;
    const constexpr std::array<A_t, 7> ANParameters::AtinyBFW;
    const constexpr std::array<restiny_t, 7> ANParameters::AtinyBFWInv;
    const constexpr std::array<A_t, 16> ANParameters::Ashort;
    const constexpr std::array<resshort_t, 16> ANParameters::AshortInv;
    const constexpr std::array<A_t, 6> ANParameters::AshortBFW;
    const constexpr std::array<resshort_t, 6> ANParameters::AshortBFWInv;
    const constexpr std::array<A_t, 16> ANParameters::Aint;
    const constexpr std::array<resint_t, 16> ANParameters::AintInv;
    const constexpr std::array<A_t, 5> ANParameters::AintBFW;
    const constexpr std::array<resint_t, 5> ANParameters::AintBFWInv;

    const restiny_t v2_restiny_t::dhm_emptykey = std::numeric_limits<restiny_t>::max();
    const restiny_t v2_restiny_t::dhm_deletedkey = std::numeric_limits<restiny_t>::max() - 1;
    const restiny_t v2_restiny_t::UNENC_MIN = 0xFF80;
    const restiny_t v2_restiny_t::UNENC_MAX = 0x007F;
    const restiny_t v2_restiny_t::UNENC_MAX_U = 0x00FF;

    const resshort_t v2_resshort_t::dhm_emptykey = std::numeric_limits<resshort_t>::max();
    const resshort_t v2_resshort_t::dhm_deletedkey = std::numeric_limits<resshort_t>::max() - 1;
    const resshort_t v2_resshort_t::UNENC_MIN = 0xFFFF8000;
    const resshort_t v2_resshort_t::UNENC_MAX = 0x00007FFF;
    const resshort_t v2_resshort_t::UNENC_MAX_U = 0x0000FFFF;

    const resint_t v2_resint_t::dhm_emptykey = std::numeric_limits<resint_t>::max();
    const resint_t v2_resint_t::dhm_deletedkey = std::numeric_limits<resint_t>::max() - 1;
    const resint_t v2_resint_t::UNENC_MIN = 0xFFFFFFFF80000000ull;
    const resint_t v2_resint_t::UNENC_MAX = 0x000000007FFFFFFFull;
    const resint_t v2_resint_t::UNENC_MAX_U = 0x00000000FFFFFFFFull;

    const resbigint_t v2_resbigint_t::dhm_emptykey = std::numeric_limits<resbigint_t>::max();
    const resbigint_t v2_resbigint_t::dhm_deletedkey = std::numeric_limits<resbigint_t>::max() - 1;
    const resbigint_t v2_resbigint_t::UNENC_MIN = 0xFF80000000000000ull;
    const resbigint_t v2_resbigint_t::UNENC_MAX = 0x007FFFFFFFFFFFFFull;
    const resbigint_t v2_resbigint_t::UNENC_MAX_U = 0x00FFFFFFFFFFFFFFull;

    const resoid_t v2_resoid_t::dhm_emptykey = std::numeric_limits<resoid_t>::max();
    const resoid_t v2_resoid_t::dhm_deletedkey = std::numeric_limits<resoid_t>::max() - 1;
    const resoid_t v2_resoid_t::UNENC_MIN = 0xFF80000000000000ull;
    const resoid_t v2_resoid_t::UNENC_MAX = 0x007FFFFFFFFFFFFFull;
    const resoid_t v2_resoid_t::UNENC_MAX_U = 0x00FFFFFFFFFFFFFFull;

    str_t v2_resstr_t::dhm_emptykey = v2_str_t::dhm_emptykey;
    str_t v2_resstr_t::dhm_deletedkey = v2_str_t::dhm_deletedkey;
    str_t v2_resstr_t::UNENC_MIN = 0x0000000000000000ull;
    str_t v2_resstr_t::UNENC_MAX = reinterpret_cast<str_t>(0xFFFFFFFFFFFFFFFFull);
    str_t v2_resstr_t::UNENC_MAX_U = reinterpret_cast<str_t>(0xFFFFFFFFFFFFFFFFull);

    cstrc_t TypeMap<v2_tinyint_t>::TYPENAME = "v2_tinyint_t";
    cstrc_t TypeMap<v2_shortint_t>::TYPENAME = "v2_shortint_t";
    cstrc_t TypeMap<v2_int_t>::TYPENAME = "v2_int_t";
    cstrc_t TypeMap<v2_bigint_t>::TYPENAME = "v2_bigint_t";
    cstrc_t TypeMap<v2_oid_t>::TYPENAME = "v2_oid_t";
    cstrc_t TypeMap<v2_void_t>::TYPENAME = "v2_void_t";
    cstrc_t TypeMap<v2_str_t>::TYPENAME = "v2_str_t";
    cstrc_t TypeMap<v2_fixed_t>::TYPENAME = "v2_fixed_t";
    cstrc_t TypeMap<v2_char_t>::TYPENAME = "v2_char_t";
    cstrc_t TypeMap<v2_id_t>::TYPENAME = "v2_id_t";
    cstrc_t TypeMap<v2_size_t>::TYPENAME = "v2_size_t";

    cstrc_t TypeMap<v2_restiny_t>::TYPENAME = "v2_restiny_t";
    cstrc_t TypeMap<v2_resshort_t>::TYPENAME = "v2_resshort_t";
    cstrc_t TypeMap<v2_resint_t>::TYPENAME = "v2_resint_t";
    cstrc_t TypeMap<v2_resbigint_t>::TYPENAME = "v2_resbigint_t";
    cstrc_t TypeMap<v2_resoid_t>::TYPENAME = "v2_resoid_t";
    cstrc_t TypeMap<v2_resstr_t>::TYPENAME = "v2_resstr_t";

}
