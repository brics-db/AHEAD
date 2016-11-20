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
 * File:   resilience.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 29. Juli 2016, 12:09
 */

#include <limits>

#include <util/resilience.hpp>

const restiny_t v2_restiny_t::dhm_emptykey = std::numeric_limits<restiny_t>::max();
const restiny_t v2_restiny_t::dhm_deletedkey = std::numeric_limits<restiny_t>::max() - 1;
const restiny_t v2_restiny_t::A_UNENC_MIN = 0xFF80;
const restiny_t v2_restiny_t::A_UNENC_MAX = 0x007F;
const restiny_t v2_restiny_t::A_UNENC_MAX_U = 0x00FF;
const restiny_t v2_restiny_t::A = 233;
const restiny_t v2_restiny_t::A_INV = 0xd759;
// const restiny_t v2_restiny_t::A2 = 55831;
// const restiny_t v2_restiny_t::A2_INV = 0x4dfffda7;

const resshort_t v2_resshort_t::dhm_emptykey = std::numeric_limits<resshort_t>::max();
const resshort_t v2_resshort_t::dhm_deletedkey = std::numeric_limits<resshort_t>::max() - 1;
const resshort_t v2_resshort_t::A_UNENC_MIN = 0xFFFF8000;
const resshort_t v2_resshort_t::A_UNENC_MAX = 0x00007FFF;
const resshort_t v2_resshort_t::A_UNENC_MAX_U = 0x0000FFFF;
const resshort_t v2_resshort_t::A = 233;
const resshort_t v2_resshort_t::A_INV = 0x1fdcd759;
// const resshort_t v2_resshort_t::A2 = 63877;
// const resshort_t v2_resshort_t::A2_INV = 0xd142174d;

const resint_t v2_resint_t::dhm_emptykey = std::numeric_limits<resint_t>::max();
const resint_t v2_resint_t::dhm_deletedkey = std::numeric_limits<resint_t>::max() - 1;
const resint_t v2_resint_t::A_UNENC_MIN = 0xFFFFFFFF80000000ull;
const resint_t v2_resint_t::A_UNENC_MAX = 0x000000007FFFFFFFull;
const resint_t v2_resint_t::A_UNENC_MAX_U = 0x00000000FFFFFFFFull;
const resint_t v2_resint_t::A = 225;
const resint_t v2_resint_t::A_INV = 0x0FEDCBA987654321ull;
// const resint_t v2_resint_t::A2 = 64311;
// const resint_t v2_resint_t::A2_INV = 0xAA86FFFEFB1FAA87ull;

const resbigint_t v2_resbigint_t::dhm_emptykey = std::numeric_limits<resbigint_t>::max();
const resbigint_t v2_resbigint_t::dhm_deletedkey = std::numeric_limits<resbigint_t>::max() - 1;
const resbigint_t v2_resbigint_t::A_UNENC_MIN = 0xFF80000000000000ull;
const resbigint_t v2_resbigint_t::A_UNENC_MAX = 0x007FFFFFFFFFFFFFull;
const resbigint_t v2_resbigint_t::A_UNENC_MAX_U = 0x00FFFFFFFFFFFFFFull;
const resbigint_t v2_resbigint_t::A = 225;
const resbigint_t v2_resbigint_t::A_INV = 0x0FEDCBA987654321ull;
// const resbigint_t v2_resbigint_t::2 = 64311;
// const resbigint_t v2_resbigint_t::2_INV = 0xAA86FFFEFB1FAA87ull;

const resoid_t v2_resoid_t::dhm_emptykey = std::numeric_limits<resoid_t>::max();
const resoid_t v2_resoid_t::dhm_deletedkey = std::numeric_limits<resoid_t>::max() - 1;
const resoid_t v2_resoid_t::A_UNENC_MIN = 0xFF80000000000000ull;
const resoid_t v2_resoid_t::A_UNENC_MAX = 0x007FFFFFFFFFFFFFull;
const resoid_t v2_resoid_t::A_UNENC_MAX_U = 0x00FFFFFFFFFFFFFFull;
const resoid_t v2_resoid_t::A = 225;
const resoid_t v2_resoid_t::A_INV = 0x0FEDCBA987654321ull;
// const resoid_t v2_resoid_t::2 = 64311;
// const resoid_t v2_resoid_t::2_INV = 0xAA86FFFEFB1FAA87ull;

cstr_t TypeMap<v2_tinyint_t>::TYPENAME = "v2_tinyint_t";
cstr_t TypeMap<v2_shortint_t>::TYPENAME = "v2_shortint_t";
cstr_t TypeMap<v2_int_t>::TYPENAME = "v2_int_t";
cstr_t TypeMap<v2_bigint_t>::TYPENAME = "v2_bigint_t";
cstr_t TypeMap<v2_oid_t>::TYPENAME = "v2_oid_t";
cstr_t TypeMap<v2_void_t>::TYPENAME = "v2_void_t";
cstr_t TypeMap<v2_restiny_t>::TYPENAME = "v2_restiny_t";
cstr_t TypeMap<v2_resshort_t>::TYPENAME = "v2_resshort_t";
cstr_t TypeMap<v2_resint_t>::TYPENAME = "v2_resint_t";
cstr_t TypeMap<v2_resbigint_t>::TYPENAME = "v2_resbigint_t";
cstr_t TypeMap<v2_resoid_t>::TYPENAME = "v2_resoid_t";
