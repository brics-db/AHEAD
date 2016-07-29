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

#include "util/resilience.hpp"

const uint64_t A = 881;
const uint64_t A_INV = 0xC87FDACE4F9E5D91;
// The min / max encoded values are computed for 64-bit (un-)signed integers as
// Signed: ENCMAX = (0x7FFFFFFF - (0x7FFFFFFF mod A)) / A
// Unsigned: ENCMAX = (0xFFFFFFFF - (0xFFFFFFFF mod A)) / A
// and for ENCMIN appropriately
const int64_t A_ENCMAX = 0x7FFFFFFF * A;
const int64_t A_ENCMIN = 0xFFFFFFFF80000000 * A;
const uint64_t A_ENCMAX_U = 0xFFFFFFFFu * A;
const uint64_t A_ENCMIN_U = 0x1u * A;

const uint64_t A2 = 64311;
const uint64_t A2_INV = 0xAA86FFFEFB1FAA87;
const int64_t A2_ENCMAX = 0x7FFFFFFF * A2;
const int64_t A2_ENCMIN = 0xFFFFFFFF80000000 * A2;
const uint64_t A2_ENCMAX_U = 0xFFFFFFFFu * A2;
const uint64_t A2_ENCMIN_U = 0x1u * A2;
