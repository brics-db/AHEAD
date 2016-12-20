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
 * File:   Operators.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 19. Dezember 2016, 14:40
 */

#include <ColumnStore.h>

#include <column_operators/Normal/miscellaneous.tcc>
#include <column_operators/Normal/select.tcc>
#include <column_operators/Normal/hashjoin.tcc>
#include <column_operators/Normal/aggregate.tcc>
#include <column_operators/Normal/groupby.tcc>

#define V2_FUNCTION_SINGLE_HEAD(FUNCTION, RETURN, HEAD) \
template RETURN<HEAD, v2_oid_t> * FUNCTION<HEAD, v2_oid_t>(BAT<HEAD, v2_oid_t> *); \
template RETURN<HEAD, v2_id_t> * FUNCTION<HEAD, v2_id_t>(BAT<HEAD, v2_id_t> *); \
template RETURN<HEAD, v2_size_t> * FUNCTION<HEAD, v2_size_t>(BAT<HEAD, v2_size_t> *); \
template RETURN<HEAD, v2_tinyint_t> * FUNCTION<HEAD, v2_tinyint_t>(BAT<HEAD, v2_tinyint_t> *); \
template RETURN<HEAD, v2_shortint_t> * FUNCTION<HEAD, v2_shortint_t>(BAT<HEAD, v2_shortint_t> *); \
template RETURN<HEAD, v2_int_t> * FUNCTION<HEAD, v2_int_t>(BAT<HEAD, v2_int_t> *); \
template RETURN<HEAD, v2_bigint_t> * FUNCTION<HEAD, v2_bigint_t>(BAT<HEAD, v2_bigint_t> *); \
template RETURN<HEAD, v2_char_t> * FUNCTION<HEAD, v2_char_t>(BAT<HEAD, v2_char_t> *); \
template RETURN<HEAD, v2_str_t> * FUNCTION<HEAD, v2_str_t>(BAT<HEAD, v2_str_t> *); \
template RETURN<HEAD, v2_fixed_t> * FUNCTION<HEAD, v2_fixed_t>(BAT<HEAD, v2_fixed_t> *);

#define V2_FUNCTION_SINGLE(FUNCTION, RETURN) \
V2_FUNCTION_SINGLE_HEAD(FUNCTION, RETURN, v2_oid_t) \
V2_FUNCTION_SINGLE_HEAD(FUNCTION, RETURN, v2_void_t) \
V2_FUNCTION_SINGLE_HEAD(FUNCTION, RETURN, v2_id_t) \
V2_FUNCTION_SINGLE_HEAD(FUNCTION, RETURN, v2_size_t) \
V2_FUNCTION_SINGLE_HEAD(FUNCTION, RETURN, v2_tinyint_t) \
V2_FUNCTION_SINGLE_HEAD(FUNCTION, RETURN, v2_shortint_t) \
V2_FUNCTION_SINGLE_HEAD(FUNCTION, RETURN, v2_int_t) \
V2_FUNCTION_SINGLE_HEAD(FUNCTION, RETURN, v2_bigint_t) \
V2_FUNCTION_SINGLE_HEAD(FUNCTION, RETURN, v2_char_t) \
V2_FUNCTION_SINGLE_HEAD(FUNCTION, RETURN, v2_str_t) \
V2_FUNCTION_SINGLE_HEAD(FUNCTION, RETURN, v2_fixed_t)

#define V2_CLASS_SINGLE_HEAD(NAME, HEAD) \
template class NAME <HEAD, v2_oid_t>; \
template class NAME <HEAD, v2_id_t>; \
template class NAME <HEAD, v2_size_t>; \
template class NAME <HEAD, v2_tinyint_t>; \
template class NAME <HEAD, v2_shortint_t>; \
template class NAME <HEAD, v2_int_t>; \
template class NAME <HEAD, v2_bigint_t>; \
template class NAME <HEAD, v2_char_t>; \
template class NAME <HEAD, v2_str_t>; \
template class NAME <HEAD, v2_fixed_t>;

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
template struct NAME <HEAD, v2_oid_t>; \
template struct NAME <HEAD, v2_id_t>; \
template struct NAME <HEAD, v2_size_t>; \
template struct NAME <HEAD, v2_tinyint_t>; \
template struct NAME <HEAD, v2_shortint_t>; \
template struct NAME <HEAD, v2_int_t>; \
template struct NAME <HEAD, v2_bigint_t>; \
template struct NAME <HEAD, v2_char_t>; \
template struct NAME <HEAD, v2_str_t>; \
template struct NAME <HEAD, v2_fixed_t>;

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

namespace v2 {
    namespace bat {
        namespace ops {
            namespace Private {
                V2_STRUCT_SINGLE (copy0)
            }

            V2_FUNCTION_SINGLE (copy, TempBAT)
        }
    }
}
