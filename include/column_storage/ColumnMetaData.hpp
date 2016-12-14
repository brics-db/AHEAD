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
 * File:   ColumnMetaData.hpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 9. Dezember 2016, 12:26
 */

#ifndef COLUMNMETADATA_HPP
#define COLUMNMETADATA_HPP

#include <cinttypes>

struct ColumnMetaData {

    uint32_t width;
    uint64_t seqbase;
    uint16_t isEncoded; // encode as 9-out-of-16
    uint16_t AN_A; // at most 16 bit A's for now
    uint64_t AN_Ainv;
    uint64_t AN_unencMaxU;
    int64_t AN_unencMinS;

    ColumnMetaData () : width (0), seqbase (0), isEncoded (0), AN_A (0), AN_Ainv (0), AN_unencMaxU (0), AN_unencMinS (0) {
    }

    ColumnMetaData (unsigned int width) : width (width), seqbase (0), isEncoded (0), AN_A (0), AN_Ainv (0), AN_unencMaxU (0), AN_unencMinS (0) {
    }

    ColumnMetaData (uint32_t width, uint16_t AN_A, uint64_t AN_Ainv, uint64_t AN_unencMaxU, int64_t AN_unencMinS) : width (width), seqbase (0), isEncoded (0xFFFF), AN_A (AN_A), AN_Ainv (AN_Ainv), AN_unencMaxU (AN_unencMaxU), AN_unencMinS (AN_unencMinS) {
    }
};

#endif /* COLUMNMETADATA_HPP */
