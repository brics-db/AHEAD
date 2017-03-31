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
 * File:   Operators.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 19. Dezember 2016, 14:40
 */

#include <column_operators/Operators.hpp>

namespace v2 {
    namespace bat {
        namespace ops {

            template TempBAT<v2_void_t, v2_oid_t>* copy(BAT<v2_void_t, v2_oid_t>* arg);
            template TempBAT<v2_void_t, v2_id_t>* copy(BAT<v2_void_t, v2_id_t>* arg);
            template TempBAT<v2_void_t, v2_size_t>* copy(BAT<v2_void_t, v2_size_t>* arg);
            template TempBAT<v2_void_t, v2_tinyint_t>* copy(BAT<v2_void_t, v2_tinyint_t>* arg);
            template TempBAT<v2_void_t, v2_shortint_t>* copy(BAT<v2_void_t, v2_shortint_t>* arg);
            template TempBAT<v2_void_t, v2_int_t>* copy(BAT<v2_void_t, v2_int_t>* arg);
            template TempBAT<v2_void_t, v2_bigint_t>* copy(BAT<v2_void_t, v2_bigint_t>* arg);
            template TempBAT<v2_void_t, v2_str_t>* copy(BAT<v2_void_t, v2_str_t>* arg);
            template TempBAT<v2_void_t, v2_restiny_t>* copy(BAT<v2_void_t, v2_restiny_t>* arg);
            template TempBAT<v2_void_t, v2_resshort_t>* copy(BAT<v2_void_t, v2_resshort_t>* arg);
            template TempBAT<v2_void_t, v2_resint_t>* copy(BAT<v2_void_t, v2_resint_t>* arg);
            template TempBAT<v2_void_t, v2_resbigint_t>* copy(BAT<v2_void_t, v2_resbigint_t>* arg);
            template TempBAT<v2_void_t, v2_resstr_t>* copy(BAT<v2_void_t, v2_resstr_t>* arg);

#define V2_SELECT2_SUB(SELECT, V2TYPE, V2SELECTTYPE, TYPE) \
template BAT<v2_oid_t, V2SELECTTYPE>* select<std::greater, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1, TYPE && th2); \
template BAT<v2_oid_t, V2SELECTTYPE>* select<std::greater_equal, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1, TYPE && th2); \
template BAT<v2_oid_t, V2SELECTTYPE>* select<std::equal_to, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1, TYPE && th2); \
template BAT<v2_oid_t, V2SELECTTYPE>* select<std::less_equal, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1, TYPE && th2); \
template BAT<v2_oid_t, V2SELECTTYPE>* select<std::less, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1, TYPE && th2);

#define V2_SELECT2(V2TYPE, V2SELECTTYPE, TYPE) \
V2_SELECT2_SUB(std::greater, V2TYPE, V2SELECTTYPE, TYPE) \
V2_SELECT2_SUB(std::greater_equal, V2TYPE, V2SELECTTYPE, TYPE) \
V2_SELECT2_SUB(std::equal_to, V2TYPE, V2SELECTTYPE, TYPE) \
V2_SELECT2_SUB(std::less_equal, V2TYPE, V2SELECTTYPE, TYPE) \
V2_SELECT2_SUB(std::less, V2TYPE, V2SELECTTYPE, TYPE)

#define V2_SELECT(V2TYPE, V2SELECTTYPE, TYPE) \
template BAT<v2_oid_t, V2SELECTTYPE>* select<std::greater, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1); \
template BAT<v2_oid_t, V2SELECTTYPE>* select<std::greater_equal, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1); \
template BAT<v2_oid_t, V2SELECTTYPE>* select<std::equal_to, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1); \
template BAT<v2_oid_t, V2SELECTTYPE>* select<std::less_equal, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1); \
template BAT<v2_oid_t, V2SELECTTYPE>* select<std::less, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1); \
V2_SELECT2(V2TYPE, V2SELECTTYPE, TYPE)

            V2_SELECT(v2_tinyint_t, v2_tinyint_t, tinyint_t)
            V2_SELECT(v2_shortint_t, v2_shortint_t, shortint_t)
            V2_SELECT(v2_int_t, v2_int_t, int_t)
            V2_SELECT(v2_bigint_t, v2_bigint_t, bigint_t)
            V2_SELECT(v2_str_t, v2_str_t, str_t)
            V2_SELECT(v2_restiny_t, v2_restiny_t, restiny_t)
            V2_SELECT(v2_resshort_t, v2_resshort_t, resshort_t)
            V2_SELECT(v2_resint_t, v2_resint_t, resint_t)
            V2_SELECT(v2_resbigint_t, v2_resbigint_t, resbigint_t)
            V2_SELECT(v2_resstr_t, v2_resstr_t, str_t)

#undef V2_SELECT
#undef V2_SELECT2
#undef V2_SELECT2_SUB

        }
    }
}
