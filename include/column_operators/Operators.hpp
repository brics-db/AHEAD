// Copyright (c) 2010 Dirk Habich
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

/***
 * @author Dirk Habich
 */
#ifndef OPERATORS_H
#define OPERATORS_H

#include <column_storage/Storage.hpp>
#include <column_operators/Normal/miscellaneous.tcc>
#include <column_operators/Normal/select.tcc>
#include <column_operators/Normal/hashjoin.tcc>
#include <column_operators/Normal/matchjoin.tcc>
#include <column_operators/Normal/aggregate.tcc>
#include <column_operators/Normal/groupby.tcc>

namespace ahead {
    namespace bat {
        namespace ops {

            extern template TempBAT<v2_void_t, v2_oid_t>* copy(BAT<v2_void_t, v2_oid_t>* arg);
            extern template TempBAT<v2_void_t, v2_id_t>* copy(BAT<v2_void_t, v2_id_t>* arg);
            extern template TempBAT<v2_void_t, v2_size_t>* copy(BAT<v2_void_t, v2_size_t>* arg);
            extern template TempBAT<v2_void_t, v2_tinyint_t>* copy(BAT<v2_void_t, v2_tinyint_t>* arg);
            extern template TempBAT<v2_void_t, v2_shortint_t>* copy(BAT<v2_void_t, v2_shortint_t>* arg);
            extern template TempBAT<v2_void_t, v2_int_t>* copy(BAT<v2_void_t, v2_int_t>* arg);
            extern template TempBAT<v2_void_t, v2_bigint_t>* copy(BAT<v2_void_t, v2_bigint_t>* arg);
            extern template TempBAT<v2_void_t, v2_str_t>* copy(BAT<v2_void_t, v2_str_t>* arg);

#define V2_SELECT2_SUB(SELECT, V2TYPE, V2SELECTTYPE, TYPE) \
extern template BAT<v2_oid_t, V2SELECTTYPE>* select<std::greater, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1, TYPE && th2); \
extern template BAT<v2_oid_t, V2SELECTTYPE>* select<std::greater_equal, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1, TYPE && th2); \
extern template BAT<v2_oid_t, V2SELECTTYPE>* select<std::equal_to, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1, TYPE && th2); \
extern template BAT<v2_oid_t, V2SELECTTYPE>* select<std::less_equal, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1, TYPE && th2); \
extern template BAT<v2_oid_t, V2SELECTTYPE>* select<std::less, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1, TYPE && th2);

#define V2_SELECT2(V2TYPE, V2SELECTTYPE, TYPE) \
V2_SELECT2_SUB(std::greater, V2TYPE, V2SELECTTYPE, TYPE) \
V2_SELECT2_SUB(std::greater_equal, V2TYPE, V2SELECTTYPE, TYPE) \
V2_SELECT2_SUB(std::equal_to, V2TYPE, V2SELECTTYPE, TYPE) \
V2_SELECT2_SUB(std::less_equal, V2TYPE, V2SELECTTYPE, TYPE) \
V2_SELECT2_SUB(std::less, V2TYPE, V2SELECTTYPE, TYPE)

#define V2_SELECT(V2TYPE, V2SELECTTYPE, TYPE) \
extern template BAT<v2_oid_t, V2SELECTTYPE>* select<std::greater, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1); \
extern template BAT<v2_oid_t, V2SELECTTYPE>* select<std::greater_equal, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1); \
extern template BAT<v2_oid_t, V2SELECTTYPE>* select<std::equal_to, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1); \
extern template BAT<v2_oid_t, V2SELECTTYPE>* select<std::less_equal, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1); \
extern template BAT<v2_oid_t, V2SELECTTYPE>* select<std::less, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1); \
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

#endif
