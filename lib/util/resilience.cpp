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

#include <util/resilience.hpp>

const restiny_t A_TINY_UNENC_MAX = 0x007F;
const restiny_t A_TINY_UNENC_MIN = 0xFF80;
const restiny_t A_TINY_UNENC_MAX_U = 0x00FF;
const restiny_t A_TINY_UNENC_MIN_U = 0x0000;
const restiny_t A_TINY = 233;
const restiny_t A_TINY_INV = 0xd759;
// const restiny_t A2_TINY = 55831;
// const restiny_t A2_TINY_INV = 0x4dfffda7;

const resshort_t A_SHORT_UNENC_MAX = 0x00007FFF;
const resshort_t A_SHORT_UNENC_MIN = 0xFFFF8000;
const resshort_t A_SHORT_UNENC_MAX_U = 0x0000FFFF;
const resshort_t A_SHORT_UNENC_MIN_U = 0x00000000;
const resshort_t A_SHORT = 233;
const resshort_t A_SHORT_INV = 0x1fdcd759;
// const resshort_t A2_SHORT = 63877;
// const resshort_t A2_SHORT_INV = 0xd142174d;

const resint_t A_INT_UNENC_MAX = 0x000000007FFFFFFFull;
const resint_t A_INT_UNENC_MIN = 0xFFFFFFFF80000000ull;
const resint_t A_INT_UNENC_MAX_U = 0x00000000FFFFFFFFull;
const resint_t A_INT_UNENC_MIN_U = 0x0000000000000000ull;
const resint_t A_INT = 225;
const resint_t A_INT_INV = 0x0FEDCBA987654321ull;
// const resint_t A2_INT = 64311;
// const resint_t A2_INT_INV = 0xAA86FFFEFB1FAA87ull;
