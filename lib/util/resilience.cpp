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

/*
const std::array<uint16_t, 16> ANParameters::Atiny = {1, 3, 7, 13, 29, 59, 115, 233, 233, 233, 233, 233, 233, 233, 233, 233}; //, 487, 857, 1939, 3813, 7463, 13963, 27247, 55831};
const std::array<restiny_t, 16> ANParameters::AtinyInv = {0x0001, 0xaaab, 0x6db7, 0x4ec5, 0xc235, 0xd8f3, 0xa4bb, 0xd759, 0xd759, 0xd759, 0xd759, 0xd759, 0xd759, 0xd759, 0xd759, 0xd759};
const std::array<uint16_t, 16> ANParameters::Ashort = {1, 3, 7, 13, 29, 61, 119, 233, 463, 947, 1939, 3349, 7785, 14781, 28183, 63877};
const std::array<resshort_t, 16> ANParameters::AshortInv = {0x00000001, 0xaaaaaaab, 0xb6db6db7, 0xc4ec4ec5, 0x4f72c235, 0xc10c9715, 0x46fdd947, 0x1fdcd759, 0xab67652f, 0xff30637b, 0xbc452e9b, 0x21b5da3d, 0x392f51d9, 0x1abdc995, 0xab2da9a7, 0xd142174d};
const std::array<uint16_t, 16> ANParameters::Aint = {1, 3, 7, 15, 21, 55, 125, 225, 445, 881, 2029, 3565, 7947, 16041, 28691, 64311};
const std::array<resint_t, 16> ANParameters::AintInv = {1, 0xAAAAAAAAAAAAAAAB, 0x6DB6DB6DB6DB6DB7, 0xEEEEEEEEEEEEEEEF, 0xCF3CF3CF3CF3CF3D, 0x6FB586FB586FB587, 0x1CAC083126E978D5, 0xFEDCBA987654321, 0x64194FF6CBA64195, 0xC87FDACE4F9E5D91, 0x49AEFF9F19DD6DE5, 0xBF4CC39BC11857E5, 0xFDD779BC079A34A3, 0xC94EE2C7649F4599, 0xBC4B7C5655ECDA1B, 0xAA86FFFEFB1FAA87};
 */
const constexpr std::array<uint16_t, 16> ANParameters::Atiny;
const constexpr std::array<restiny_t, 16> ANParameters::AtinyInv;
const constexpr std::array<uint16_t, 16> ANParameters::Ashort;
const constexpr std::array<resshort_t, 16> ANParameters::AshortInv;
const constexpr std::array<uint16_t, 16> ANParameters::Aint;
const constexpr std::array<resint_t, 16> ANParameters::AintInv;

const restiny_t v2_restiny_t::dhm_emptykey = std::numeric_limits<restiny_t>::max ();
const restiny_t v2_restiny_t::dhm_deletedkey = std::numeric_limits<restiny_t>::max () - 1;
const restiny_t v2_restiny_t::UNENC_MIN = 0xFF80;
const restiny_t v2_restiny_t::UNENC_MAX = 0x007F;
const restiny_t v2_restiny_t::UNENC_MAX_U = 0x00FF;
// const std::array<uint16_t, 16> * v2_restiny_t::As = &ANParameters::Atiny;
// const std::array<restiny_t, 16> * v2_restiny_t::Ainvs = &ANParameters::AtinyInv;

const resshort_t v2_resshort_t::dhm_emptykey = std::numeric_limits<resshort_t>::max ();
const resshort_t v2_resshort_t::dhm_deletedkey = std::numeric_limits<resshort_t>::max () - 1;
const resshort_t v2_resshort_t::UNENC_MIN = 0xFFFF8000;
const resshort_t v2_resshort_t::UNENC_MAX = 0x00007FFF;
const resshort_t v2_resshort_t::UNENC_MAX_U = 0x0000FFFF;
// const std::array<uint16_t, 16> * v2_resshort_t::As = &ANParameters::Ashort;
// const std::array<resshort_t, 16> * v2_resshort_t::Ainvs = &ANParameters::AshortInv;

const resint_t v2_resint_t::dhm_emptykey = std::numeric_limits<resint_t>::max ();
const resint_t v2_resint_t::dhm_deletedkey = std::numeric_limits<resint_t>::max () - 1;
const resint_t v2_resint_t::UNENC_MIN = 0xFFFFFFFF80000000ull;
const resint_t v2_resint_t::UNENC_MAX = 0x000000007FFFFFFFull;
const resint_t v2_resint_t::UNENC_MAX_U = 0x00000000FFFFFFFFull;
// const std::array<uint16_t, 16> * v2_resint_t::As = &ANParameters::Aint;
// const std::array<resint_t, 16> * v2_resint_t::Ainvs = &ANParameters::AintInv;

const resbigint_t v2_resbigint_t::dhm_emptykey = std::numeric_limits<resbigint_t>::max ();
const resbigint_t v2_resbigint_t::dhm_deletedkey = std::numeric_limits<resbigint_t>::max () - 1;
const resbigint_t v2_resbigint_t::UNENC_MIN = 0xFF80000000000000ull;
const resbigint_t v2_resbigint_t::UNENC_MAX = 0x007FFFFFFFFFFFFFull;
const resbigint_t v2_resbigint_t::UNENC_MAX_U = 0x00FFFFFFFFFFFFFFull;
// const std::array<uint16_t, 16> * v2_resbigint_t::As = &ANParameters::Aint;
// const std::array<resbigint_t, 16> * v2_resbigint_t::Ainvs = &ANParameters::AintInv;

const resoid_t v2_resoid_t::dhm_emptykey = std::numeric_limits<resoid_t>::max ();
const resoid_t v2_resoid_t::dhm_deletedkey = std::numeric_limits<resoid_t>::max () - 1;
const resoid_t v2_resoid_t::UNENC_MIN = 0xFF80000000000000ull;
const resoid_t v2_resoid_t::UNENC_MAX = 0x007FFFFFFFFFFFFFull;
const resoid_t v2_resoid_t::UNENC_MAX_U = 0x00FFFFFFFFFFFFFFull;
// const std::array<uint16_t, 16> * v2_resoid_t::As = &ANParameters::Aint;
// const std::array<resoid_t, 16> * v2_resoid_t::Ainvs = &ANParameters::AintInv;

cstr_t v2_resstr_t::dhm_emptykey = v2_str_t::dhm_emptykey;
cstr_t v2_resstr_t::dhm_deletedkey = v2_str_t::dhm_deletedkey;
str_t v2_resstr_t::UNENC_MIN = 0x0000000000000000ull;
str_t v2_resstr_t::UNENC_MAX = reinterpret_cast<str_t>(0xFFFFFFFFFFFFFFFFull);
str_t v2_resstr_t::UNENC_MAX_U = reinterpret_cast<str_t>(0xFFFFFFFFFFFFFFFFull);

cstr_t TypeMap<v2_tinyint_t>::TYPENAME = "v2_tinyint_t";
cstr_t TypeMap<v2_shortint_t>::TYPENAME = "v2_shortint_t";
cstr_t TypeMap<v2_int_t>::TYPENAME = "v2_int_t";
cstr_t TypeMap<v2_bigint_t>::TYPENAME = "v2_bigint_t";
cstr_t TypeMap<v2_oid_t>::TYPENAME = "v2_oid_t";
cstr_t TypeMap<v2_void_t>::TYPENAME = "v2_void_t";
cstr_t TypeMap<v2_str_t>::TYPENAME = "v2_str_t";
cstr_t TypeMap<v2_fixed_t>::TYPENAME = "v2_fixed_t";
cstr_t TypeMap<v2_char_t>::TYPENAME = "v2_char_t";
cstr_t TypeMap<v2_id_t>::TYPENAME = "v2_id_t";
cstr_t TypeMap<v2_size_t>::TYPENAME = "v2_size_t";

cstr_t TypeMap<v2_restiny_t>::TYPENAME = "v2_restiny_t";
cstr_t TypeMap<v2_resshort_t>::TYPENAME = "v2_resshort_t";
cstr_t TypeMap<v2_resint_t>::TYPENAME = "v2_resint_t";
cstr_t TypeMap<v2_resbigint_t>::TYPENAME = "v2_resbigint_t";
cstr_t TypeMap<v2_resoid_t>::TYPENAME = "v2_resoid_t";
cstr_t TypeMap<v2_resstr_t>::TYPENAME = "v2_resstr_t";
