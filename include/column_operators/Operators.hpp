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

#include <ColumnStore.h>

#include <column_operators/Normal/miscellaneous.tcc>
#include <column_operators/Normal/select.tcc>
#include <column_operators/Normal/hashjoin.tcc>
#include <column_operators/Normal/matchjoin.tcc>
#include <column_operators/Normal/aggregate.tcc>
#include <column_operators/Normal/groupby.tcc>

#define V2_FUNCTION_SINGLE_HEAD(NAME, RETURN, HEAD) \
extern template RETURN<HEAD, v2_oid_t> * NAME<HEAD, v2_oid_t>(BAT<HEAD, v2_oid_t> * arg); \
extern template RETURN<HEAD, v2_id_t> * NAME<HEAD, v2_id_t>(BAT<HEAD, v2_id_t> * arg); \
extern template RETURN<HEAD, v2_size_t> * NAME<HEAD, v2_size_t>(BAT<HEAD, v2_size_t> * arg); \
extern template RETURN<HEAD, v2_tinyint_t> * NAME<HEAD, v2_tinyint_t>(BAT<HEAD, v2_tinyint_t> * arg); \
extern template RETURN<HEAD, v2_shortint_t> * NAME<HEAD, v2_shortint_t>(BAT<HEAD, v2_shortint_t> * arg); \
extern template RETURN<HEAD, v2_int_t> * NAME<HEAD, v2_int_t>(BAT<HEAD, v2_int_t> * arg); \
extern template RETURN<HEAD, v2_bigint_t> * NAME<HEAD, v2_bigint_t>(BAT<HEAD, v2_bigint_t> * arg); \
extern template RETURN<HEAD, v2_char_t> * NAME<HEAD, v2_char_t>(BAT<HEAD, v2_char_t> * arg); \
extern template RETURN<HEAD, v2_str_t> * NAME<HEAD, v2_str_t>(BAT<HEAD, v2_str_t> * arg); \
extern template RETURN<HEAD, v2_fixed_t> * NAME<HEAD, v2_fixed_t>(BAT<HEAD, v2_fixed_t> * arg);

#define V2_FUNCTION_SINGLE(NAME, RETURN) \
V2_FUNCTION_SINGLE_HEAD(NAME, RETURN, v2_oid_t) \
V2_FUNCTION_SINGLE_HEAD(NAME, RETURN, v2_void_t) \
V2_FUNCTION_SINGLE_HEAD(NAME, RETURN, v2_id_t) \
V2_FUNCTION_SINGLE_HEAD(NAME, RETURN, v2_size_t) \
V2_FUNCTION_SINGLE_HEAD(NAME, RETURN, v2_tinyint_t) \
V2_FUNCTION_SINGLE_HEAD(NAME, RETURN, v2_shortint_t) \
V2_FUNCTION_SINGLE_HEAD(NAME, RETURN, v2_int_t) \
V2_FUNCTION_SINGLE_HEAD(NAME, RETURN, v2_bigint_t) \
V2_FUNCTION_SINGLE_HEAD(NAME, RETURN, v2_char_t) \
V2_FUNCTION_SINGLE_HEAD(NAME, RETURN, v2_str_t) \
V2_FUNCTION_SINGLE_HEAD(NAME, RETURN, v2_fixed_t)

#define V2_CLASS_SINGLE_HEAD(NAME, HEAD) \
extern template class NAME <HEAD, v2_oid_t>; \
extern template class NAME <HEAD, v2_id_t>; \
extern template class NAME <HEAD, v2_size_t>; \
extern template class NAME <HEAD, v2_tinyint_t>; \
extern template class NAME <HEAD, v2_shortint_t>; \
extern template class NAME <HEAD, v2_int_t>; \
extern template class NAME <HEAD, v2_bigint_t>; \
extern template class NAME <HEAD, v2_char_t>; \
extern template class NAME <HEAD, v2_str_t>; \
extern template class NAME <HEAD, v2_fixed_t>;

#define V2_CLASS_SINGLE(NAME) \
V2_CLASS_SINGLE_HEAD(NAME, v2_oid_t) \
V2_CLASS_SINGLE_HEAD(NAME, v2_void_t) \
V2_CLASS_SINGLE_HEAD(NAME, v2_id_t) \
V2_CLASS_SINGLE_HEAD(NAME, v2_size_t) \
V2_CLASS_SINGLE_HEAD(NAME, v2_tinyint_t) \
V2_CLASS_SINGLE_HEAD(NAME, v2_shortint_t) \
V2_CLASS_SINGLE_HEAD(NAME, v2_int_t) \
V2_CLASS_SINGLE_HEAD(NAME, v2_bigint_t) \
V2_CLASS_SINGLE_HEAD(NAME, v2_char_t) \
V2_CLASS_SINGLE_HEAD(NAME, v2_str_t) \
V2_CLASS_SINGLE_HEAD(NAME, v2_fixed_t)

#define V2_STRUCT_SINGLE_HEAD(NAME, HEAD) \
extern template struct NAME <HEAD, v2_oid_t>; \
extern template struct NAME <HEAD, v2_id_t>; \
extern template struct NAME <HEAD, v2_size_t>; \
extern template struct NAME <HEAD, v2_tinyint_t>; \
extern template struct NAME <HEAD, v2_shortint_t>; \
extern template struct NAME <HEAD, v2_int_t>; \
extern template struct NAME <HEAD, v2_bigint_t>; \
extern template struct NAME <HEAD, v2_char_t>; \
extern template struct NAME <HEAD, v2_str_t>; \
extern template struct NAME <HEAD, v2_fixed_t>;

#define V2_STRUCT_SINGLE(NAME) \
V2_STRUCT_SINGLE_HEAD(NAME, v2_oid_t) \
V2_STRUCT_SINGLE_HEAD(NAME, v2_void_t) \
V2_STRUCT_SINGLE_HEAD(NAME, v2_id_t) \
V2_STRUCT_SINGLE_HEAD(NAME, v2_size_t) \
V2_STRUCT_SINGLE_HEAD(NAME, v2_tinyint_t) \
V2_STRUCT_SINGLE_HEAD(NAME, v2_shortint_t) \
V2_STRUCT_SINGLE_HEAD(NAME, v2_int_t) \
V2_STRUCT_SINGLE_HEAD(NAME, v2_bigint_t) \
V2_STRUCT_SINGLE_HEAD(NAME, v2_char_t) \
V2_STRUCT_SINGLE_HEAD(NAME, v2_str_t) \
V2_STRUCT_SINGLE_HEAD(NAME, v2_fixed_t)

#define V2_SELECT(V2TYPE, TYPE) \
extern template BAT<v2_oid_t, V2TYPE>* select<std::greater, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1); \
extern template BAT<v2_oid_t, V2TYPE>* select<std::greater_equal, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1); \
extern template BAT<v2_oid_t, V2TYPE>* select<std::equal_to, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1); \
extern template BAT<v2_oid_t, V2TYPE>* select<std::less_equal, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1); \
extern template BAT<v2_oid_t, V2TYPE>* select<std::less, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1);

namespace v2 {
    namespace bat {
        namespace ops {
            namespace Private {
                V2_STRUCT_SINGLE (copy0)

                extern template struct Selection1<std::greater<tinyint_t>, v2_void_t, v2_tinyint_t>;
                extern template struct Selection1<std::greater_equal<tinyint_t>, v2_void_t, v2_tinyint_t>;
                extern template struct Selection1<std::equal_to<tinyint_t>, v2_void_t, v2_tinyint_t>;
                extern template struct Selection1<std::less_equal<tinyint_t>, v2_void_t, v2_tinyint_t>;
                extern template struct Selection1<std::less<tinyint_t>, v2_void_t, v2_tinyint_t>;

                extern template struct Selection1<std::greater<shortint_t>, v2_void_t, v2_shortint_t>;
                extern template struct Selection1<std::greater_equal<shortint_t>, v2_void_t, v2_shortint_t>;
                extern template struct Selection1<std::equal_to<shortint_t>, v2_void_t, v2_shortint_t>;
                extern template struct Selection1<std::less_equal<shortint_t>, v2_void_t, v2_shortint_t>;
                extern template struct Selection1<std::less<shortint_t>, v2_void_t, v2_shortint_t>;

                extern template struct Selection1<std::greater<int_t>, v2_void_t, v2_int_t>;
                extern template struct Selection1<std::greater_equal<int_t>, v2_void_t, v2_int_t>;
                extern template struct Selection1<std::equal_to<int_t>, v2_void_t, v2_int_t>;
                extern template struct Selection1<std::less_equal<int_t>, v2_void_t, v2_int_t>;
                extern template struct Selection1<std::less<int_t>, v2_void_t, v2_int_t>;

                extern template struct Selection1<std::greater<bigint_t>, v2_void_t, v2_bigint_t>;
                extern template struct Selection1<std::greater_equal<bigint_t>, v2_void_t, v2_bigint_t>;
                extern template struct Selection1<std::equal_to<bigint_t>, v2_void_t, v2_bigint_t>;
                extern template struct Selection1<std::less_equal<bigint_t>, v2_void_t, v2_bigint_t>;
                extern template struct Selection1<std::less<bigint_t>, v2_void_t, v2_bigint_t>;
            }

            V2_FUNCTION_SINGLE (copy, TempBAT)

            V2_SELECT (v2_tinyint_t, tinyint_t)
            V2_SELECT (v2_shortint_t, shortint_t)
            V2_SELECT (v2_int_t, int_t)
            V2_SELECT (v2_bigint_t, bigint_t)
            V2_SELECT (v2_str_t, str_t)
        }
    }
}

#undef V2_SELECT
#undef V2_FUNCTION_SINGLE_HEAD
#undef V2_FUNCTION_SINGLE
#undef V2_CLASS_SINGLE_HEAD
#undef V2_CLASS_SINGLE
#undef V2_STRUCT_SINGLE_HEAD
#undef V2_STRUCT_SINGLE

#endif
