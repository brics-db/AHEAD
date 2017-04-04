// Copyright (c) 2016-2017 Till Kolditz
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
 * File:   OperatorsAN.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 31. March 2017, 13:31
 */

#include <column_operators/OperatorsAN.hpp>

namespace ahead {
    namespace bat {
        namespace ops {

            template TempBAT<v2_void_t, v2_restiny_t>* copy(BAT<v2_void_t, v2_restiny_t>* arg);
            template TempBAT<v2_void_t, v2_resshort_t>* copy(BAT<v2_void_t, v2_resshort_t>* arg);
            template TempBAT<v2_void_t, v2_resint_t>* copy(BAT<v2_void_t, v2_resint_t>* arg);
            template TempBAT<v2_void_t, v2_resbigint_t>* copy(BAT<v2_void_t, v2_resbigint_t>* arg);
            template TempBAT<v2_void_t, v2_resstr_t>* copy(BAT<v2_void_t, v2_resstr_t>* arg);

#define V2_SELECT(V2TYPE, V2SELECTTYPE, TYPE) \
template std::pair<BAT<v2_resoid_t, V2SELECTTYPE>*, std::vector<bool>*> selectAN<std::greater, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1); \
template std::pair<BAT<v2_resoid_t, V2SELECTTYPE>*, std::vector<bool>*> selectAN<std::greater_equal, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1); \
template std::pair<BAT<v2_resoid_t, V2SELECTTYPE>*, std::vector<bool>*> selectAN<std::equal_to, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1); \
template std::pair<BAT<v2_resoid_t, V2SELECTTYPE>*, std::vector<bool>*> selectAN<std::less_equal, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1); \
template std::pair<BAT<v2_resoid_t, V2SELECTTYPE>*, std::vector<bool>*> selectAN<std::less, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1);

            V2_SELECT(v2_restiny_t, v2_restiny_t, restiny_t)
            V2_SELECT(v2_resshort_t, v2_resshort_t, resshort_t)
            V2_SELECT(v2_resint_t, v2_resint_t, resint_t)
            V2_SELECT(v2_resbigint_t, v2_resbigint_t, resbigint_t)
            //V2_SELECT(v2_resstr_t, v2_resstr_t, str_t)

#undef V2_SELECT

        }
    }
}
