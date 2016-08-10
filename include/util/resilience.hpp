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
 * File:   resilience.hpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 29. Juli 2016, 12:08
 */

#ifndef RESILIENCE_HPP
#define RESILIENCE_HPP

#include <ColumnStore.h>

extern const restiny_t A_TINY_UNENC_MAX;
extern const restiny_t A_TINY_UNENC_MIN;
extern const restiny_t A_TINY_UNENC_MAX_U;
extern const restiny_t A_TINY_UNENC_MIN_U;
extern const restiny_t A_TINY;
extern const restiny_t A_TINY_INV;
extern const restiny_t A2_TINY;
extern const restiny_t A2_TINY_INV;

extern const resshort_t A_SHORT_UNENC_MAX;
extern const resshort_t A_SHORT_UNENC_MIN;
extern const resshort_t A_SHORT_UNENC_MAX_U;
extern const resshort_t A_SHORT_UNENC_MIN_U;
extern const resshort_t A_SHORT;
extern const resshort_t A_SHORT_INV;
extern const resshort_t A2_SHORT;
extern const resshort_t A2_SHORT_INV;

extern const resint_t A_INT_UNENC_MAX;
extern const resint_t A_INT_UNENC_MIN;
extern const resint_t A_INT_UNENC_MAX_U;
extern const resint_t A_INT_UNENC_MIN_U;
extern const resint_t A_INT;
extern const resint_t A_INT_INV;
extern const resint_t A2_INT;
extern const resint_t A2_INT_INV;

#endif /* RESILIENCE_HPP */
