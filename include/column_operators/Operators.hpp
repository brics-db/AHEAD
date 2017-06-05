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

/***
 * @author Dirk Habich
 * @author Till Kolditz <Till.Kolditz@gmail.com>
 */
#ifndef OPERATORS_HPP
#define OPERATORS_HPP

#include <column_storage/Storage.hpp>

namespace ahead {
    namespace bat {
        namespace ops {

            template<typename Head, typename Tail>
            TempBAT<typename Head::v2_copy_t, typename Tail::v2_copy_t> *
            copy(BAT<Head, Tail> * arg);

            template<typename H1, typename T1, typename H2, typename T2>
            BAT<typename H1::v2_select_t, typename T2::v2_select_t> *
            matchjoin(BAT<H1, T1> * arg1, BAT<H2, T2> * arg2);

            template<typename T2>
            BAT<v2_void_t, typename T2::v2_select_t> *
            fetchjoin(BAT<v2_void_t, v2_oid_t> * arg1, BAT<v2_void_t, T2> * arg2);

            template<typename H1, typename T1, typename H2, typename T2>
            BAT<typename H1::v2_select_t, typename T2::v2_select_t> *
            hashjoin(BAT<H1, T1> * arg1, BAT<H2, T2> * arg2, hash_side_t side = hash_side_t::right);

            template<typename Head, typename Tail>
            std::pair<BAT<v2_void_t, v2_oid_t> *, BAT<v2_void_t, v2_oid_t> *>
            groupby(BAT<Head, Tail>* bat);

            template<typename Head, typename Tail>
            std::pair<BAT<v2_void_t, v2_oid_t> *, BAT<v2_void_t, v2_oid_t> *>
            groupby(BAT<Head, Tail> * bat, BAT<v2_void_t, v2_oid_t> * grouping);

            template<typename V2Result, typename Head, typename Tail>
            BAT<v2_void_t, V2Result> *
            aggregate_sum_grouped(BAT<Head, Tail> * bat, BAT<v2_void_t, v2_oid_t> * grouping, size_t numGroups);

            namespace scalar {
                // SELECT
                template<template<typename > class Op, typename Head, typename Tail>
                BAT<typename Head::v2_select_t, typename Tail::v2_select_t>*
                select(BAT<Head, Tail>* arg, typename Tail::type_t && th1);

                template<template<typename > class Op1 = std::greater_equal, template<typename > class Op2 = std::less_equal, typename Head, typename Tail>
                BAT<typename Head::v2_select_t, typename Tail::v2_select_t>*
                select(BAT<Head, Tail>* arg, typename Tail::type_t && th1, typename Tail::type_t && th2);

                // AGGREGATE
                template<typename v2_result_t, typename Head, typename Tail>
                typename v2_result_t::type_t
                aggregate_sum(BAT<Head, Tail>* arg);

                template<typename Result, typename Head1, typename Tail1, typename Head2, typename Tail2>
                BAT<v2_void_t, Result>*
                aggregate_mul_sum(BAT<Head1, Tail1>* arg1, BAT<Head2, Tail2>* arg2, typename Result::type_t init = typename Result::type_t(0));
            }

            namespace sse {
                // SELECT
                template<template<typename > class Op, typename Head, typename Tail>
                BAT<typename Head::v2_select_t, typename Tail::v2_select_t>*
                select(BAT<Head, Tail>* arg, typename Tail::type_t && th1);

                template<template<typename > class Op1 = std::greater_equal, template<typename > class Op2 = std::less_equal, typename Head, typename Tail>
                BAT<typename Head::v2_select_t, typename Tail::v2_select_t>*
                select(BAT<Head, Tail>* arg, typename Tail::type_t && th1, typename Tail::type_t && th2);

                // AGGREGATE
                template<typename v2_result_t, typename Head, typename Tail>
                typename v2_result_t::type_t
                aggregate_sum(BAT<Head, Tail>* arg);

                template<typename Result, typename Head1, typename Tail1, typename Head2, typename Tail2>
                BAT<v2_void_t, Result>*
                aggregate_mul_sum(BAT<Head1, Tail1>* arg1, BAT<Head2, Tail2>* arg2, typename Result::type_t init = typename Result::type_t(0));
            }

#define V2_COPY(V2TYPE) \
extern template TempBAT<v2_void_t, V2TYPE>* copy(BAT<v2_void_t, V2TYPE>* arg); \
extern template TempBAT<v2_oid_t, V2TYPE>* copy(BAT<v2_oid_t, V2TYPE>* arg); \
extern template TempBAT<v2_id_t, V2TYPE>* copy(BAT<v2_id_t, V2TYPE>* arg); \
extern template TempBAT<v2_size_t, V2TYPE>* copy(BAT<v2_size_t, V2TYPE>* arg); \
extern template TempBAT<v2_tinyint_t, V2TYPE>* copy(BAT<v2_tinyint_t, V2TYPE>* arg); \
extern template TempBAT<v2_shortint_t, V2TYPE>* copy(BAT<v2_shortint_t, V2TYPE>* arg); \
extern template TempBAT<v2_int_t, V2TYPE>* copy(BAT<v2_int_t, V2TYPE>* arg); \
extern template TempBAT<v2_bigint_t, V2TYPE>* copy(BAT<v2_bigint_t, V2TYPE>* arg); \
extern template TempBAT<v2_str_t, V2TYPE>* copy(BAT<v2_str_t, V2TYPE>* arg); \
extern template TempBAT<v2_restiny_t, V2TYPE>* copy(BAT<v2_restiny_t, V2TYPE>* arg); \
extern template TempBAT<v2_resoid_t, V2TYPE>* copy(BAT<v2_resoid_t, V2TYPE>* arg); \
extern template TempBAT<v2_resshort_t, V2TYPE>* copy(BAT<v2_resshort_t, V2TYPE>* arg); \
extern template TempBAT<v2_resint_t, V2TYPE>* copy(BAT<v2_resint_t, V2TYPE>* arg); \
extern template TempBAT<v2_resbigint_t, V2TYPE>* copy(BAT<v2_resbigint_t, V2TYPE>* arg); \
extern template TempBAT<v2_resstr_t, V2TYPE>* copy(BAT<v2_resstr_t, V2TYPE>* arg);

            V2_COPY(v2_oid_t)
            V2_COPY(v2_id_t)
            V2_COPY(v2_size_t)
            V2_COPY(v2_tinyint_t)
            V2_COPY(v2_shortint_t)
            V2_COPY(v2_int_t)
            V2_COPY(v2_bigint_t)
            V2_COPY(v2_str_t)
            V2_COPY(v2_resoid_t)
            V2_COPY(v2_restiny_t)
            V2_COPY(v2_resshort_t)
            V2_COPY(v2_resint_t)
            V2_COPY(v2_resbigint_t)
            V2_COPY(v2_resstr_t)

#undef V2_COPY

#define V2_SELECT2_SUB2(SELECT1, SELECT2, V2TYPE) \
extern template BAT<v2_oid_t, typename V2TYPE::v2_select_t>* scalar::select<SELECT1, SELECT2, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t && th1, typename V2TYPE::type_t && th2); \
extern template BAT<v2_oid_t, typename V2TYPE::v2_select_t>* sse::select<SELECT1, SELECT2, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t && th1, typename V2TYPE::type_t && th2);

#define V2_SELECT2_SUB(SELECT, V2TYPE) \
V2_SELECT2_SUB2(std::greater, SELECT, V2TYPE) \
V2_SELECT2_SUB2(std::greater_equal, SELECT, V2TYPE) \
V2_SELECT2_SUB2(std::equal_to, SELECT, V2TYPE) \
V2_SELECT2_SUB2(std::less_equal, SELECT, V2TYPE) \
V2_SELECT2_SUB2(std::less, SELECT, V2TYPE) \
V2_SELECT2_SUB2(std::not_equal_to, SELECT, V2TYPE)

#define V2_SELECT2(V2TYPE) \
V2_SELECT2_SUB(std::greater, V2TYPE) \
V2_SELECT2_SUB(std::greater_equal, V2TYPE) \
V2_SELECT2_SUB(std::equal_to, V2TYPE) \
V2_SELECT2_SUB(std::less_equal, V2TYPE) \
V2_SELECT2_SUB(std::less, V2TYPE) \
V2_SELECT2_SUB(std::not_equal_to, V2TYPE)

#define V2_SELECT1(V2HEAD, V2TAIL) \
extern template BAT<typename V2HEAD::v2_select_t, typename V2TAIL::v2_select_t>* scalar::select<std::greater, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1); \
extern template BAT<typename V2HEAD::v2_select_t, typename V2TAIL::v2_select_t>* scalar::select<std::greater_equal, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1); \
extern template BAT<typename V2HEAD::v2_select_t, typename V2TAIL::v2_select_t>* scalar::select<std::equal_to, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1); \
extern template BAT<typename V2HEAD::v2_select_t, typename V2TAIL::v2_select_t>* scalar::select<std::less_equal, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1); \
extern template BAT<typename V2HEAD::v2_select_t, typename V2TAIL::v2_select_t>* scalar::select<std::less, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1); \
extern template BAT<typename V2HEAD::v2_select_t, typename V2TAIL::v2_select_t>* sse::select<std::greater, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1); \
extern template BAT<typename V2HEAD::v2_select_t, typename V2TAIL::v2_select_t>* sse::select<std::greater_equal, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1); \
extern template BAT<typename V2HEAD::v2_select_t, typename V2TAIL::v2_select_t>* sse::select<std::equal_to, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1); \
extern template BAT<typename V2HEAD::v2_select_t, typename V2TAIL::v2_select_t>* sse::select<std::less_equal, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1); \
extern template BAT<typename V2HEAD::v2_select_t, typename V2TAIL::v2_select_t>* sse::select<std::less, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1);

#define V2_SELECT(V2TAIL) \
V2_SELECT1(v2_void_t, V2TAIL) \
V2_SELECT1(v2_oid_t, V2TAIL) \
V2_SELECT1(v2_id_t, V2TAIL) \
V2_SELECT1(v2_size_t, V2TAIL) \
V2_SELECT1(v2_tinyint_t, V2TAIL) \
V2_SELECT1(v2_shortint_t, V2TAIL) \
V2_SELECT1(v2_int_t, V2TAIL) \
V2_SELECT1(v2_bigint_t, V2TAIL) \
V2_SELECT1(v2_str_t, V2TAIL) \
V2_SELECT1(v2_resoid_t, V2TAIL) \
V2_SELECT1(v2_restiny_t, V2TAIL) \
V2_SELECT1(v2_resshort_t, V2TAIL) \
V2_SELECT1(v2_resint_t, V2TAIL) \
V2_SELECT1(v2_resbigint_t, V2TAIL) \
V2_SELECT1(v2_resstr_t, V2TAIL) \
V2_SELECT2(V2TAIL)

            V2_SELECT(v2_tinyint_t)
            V2_SELECT(v2_shortint_t)
            V2_SELECT(v2_int_t)
            V2_SELECT(v2_bigint_t)
            V2_SELECT(v2_str_t)
            V2_SELECT(v2_resoid_t)
            V2_SELECT(v2_restiny_t)
            V2_SELECT(v2_resshort_t)
            V2_SELECT(v2_resint_t)
            V2_SELECT(v2_resbigint_t)
            V2_SELECT(v2_resstr_t)

#undef V2_SELECT
#undef V2_SELECT2
#undef V2_SELECT2_SUB
#undef V2_SELECT2_SUB2

#define V2_HASHJOIN_SUB(V2TYPE_HEAD, V2TYPE_TAIL) \
extern template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * hashjoin(BAT<V2TYPE_HEAD, v2_void_t> * arg1, BAT<v2_void_t, V2TYPE_TAIL> * arg2, hash_side_t side); \
extern template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * hashjoin(BAT<V2TYPE_HEAD, v2_oid_t> * arg1, BAT<v2_oid_t, V2TYPE_TAIL> * arg2, hash_side_t side); \
extern template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * hashjoin(BAT<V2TYPE_HEAD, v2_id_t> * arg1, BAT<v2_id_t, V2TYPE_TAIL> * arg2, hash_side_t side); \
extern template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * hashjoin(BAT<V2TYPE_HEAD, v2_size_t> * arg1, BAT<v2_size_t, V2TYPE_TAIL> * arg2, hash_side_t side); \
extern template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * hashjoin(BAT<V2TYPE_HEAD, v2_tinyint_t> * arg1, BAT<v2_tinyint_t, V2TYPE_TAIL> * arg2, hash_side_t side); \
extern template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * hashjoin(BAT<V2TYPE_HEAD, v2_shortint_t> * arg1, BAT<v2_shortint_t, V2TYPE_TAIL> * arg2, hash_side_t side); \
extern template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * hashjoin(BAT<V2TYPE_HEAD, v2_int_t> * arg1, BAT<v2_int_t, V2TYPE_TAIL> * arg2, hash_side_t side); \
extern template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * hashjoin(BAT<V2TYPE_HEAD, v2_bigint_t> * arg1, BAT<v2_bigint_t, V2TYPE_TAIL> * arg2, hash_side_t side); \
extern template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * hashjoin(BAT<V2TYPE_HEAD, v2_str_t> * arg1, BAT<v2_str_t, V2TYPE_TAIL> * arg2, hash_side_t side); \
extern template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * hashjoin(BAT<V2TYPE_HEAD, v2_restiny_t> * arg1, BAT<v2_restiny_t, V2TYPE_TAIL> * arg2, hash_side_t side); \
extern template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * hashjoin(BAT<V2TYPE_HEAD, v2_resoid_t> * arg1, BAT<v2_resoid_t, V2TYPE_TAIL> * arg2, hash_side_t side); \
extern template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * hashjoin(BAT<V2TYPE_HEAD, v2_resshort_t> * arg1, BAT<v2_resshort_t, V2TYPE_TAIL> * arg2, hash_side_t side); \
extern template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * hashjoin(BAT<V2TYPE_HEAD, v2_resint_t> * arg1, BAT<v2_resint_t, V2TYPE_TAIL> * arg2, hash_side_t side); \
extern template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * hashjoin(BAT<V2TYPE_HEAD, v2_resbigint_t> * arg1, BAT<v2_resbigint_t, V2TYPE_TAIL> * arg2, hash_side_t side); \
extern template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * hashjoin(BAT<V2TYPE_HEAD, v2_resstr_t> * arg1, BAT<v2_resstr_t, V2TYPE_TAIL> * arg2, hash_side_t side);

#define V2_HASHJOIN(V2TYPE) \
V2_HASHJOIN_SUB(v2_void_t, V2TYPE) \
V2_HASHJOIN_SUB(v2_oid_t, V2TYPE) \
V2_HASHJOIN_SUB(v2_id_t, V2TYPE) \
V2_HASHJOIN_SUB(v2_size_t, V2TYPE) \
V2_HASHJOIN_SUB(v2_tinyint_t, V2TYPE) \
V2_HASHJOIN_SUB(v2_shortint_t, V2TYPE) \
V2_HASHJOIN_SUB(v2_int_t, V2TYPE) \
V2_HASHJOIN_SUB(v2_bigint_t, V2TYPE) \
V2_HASHJOIN_SUB(v2_str_t, V2TYPE) \
V2_HASHJOIN_SUB(v2_restiny_t, V2TYPE) \
V2_HASHJOIN_SUB(v2_resoid_t, V2TYPE) \
V2_HASHJOIN_SUB(v2_resshort_t, V2TYPE) \
V2_HASHJOIN_SUB(v2_resint_t, V2TYPE) \
V2_HASHJOIN_SUB(v2_resbigint_t, V2TYPE) \
V2_HASHJOIN_SUB(v2_resstr_t, V2TYPE)

            V2_HASHJOIN(v2_void_t)
            V2_HASHJOIN(v2_oid_t)
            V2_HASHJOIN(v2_id_t)
            V2_HASHJOIN(v2_size_t)
            V2_HASHJOIN(v2_tinyint_t)
            V2_HASHJOIN(v2_shortint_t)
            V2_HASHJOIN(v2_int_t)
            V2_HASHJOIN(v2_bigint_t)
            V2_HASHJOIN(v2_str_t)
            V2_HASHJOIN(v2_resoid_t)
            V2_HASHJOIN(v2_restiny_t)
            V2_HASHJOIN(v2_resshort_t)
            V2_HASHJOIN(v2_resint_t)
            V2_HASHJOIN(v2_resbigint_t)
            V2_HASHJOIN(v2_resstr_t)

#undef V2_HASHJOIN
#undef V2_HASHJOIN_SUB

#define V2_MATCHJOIN_SUB(V2TYPE_HEAD, V2TYPE_TAIL) \
extern template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * matchjoin(BAT<V2TYPE_HEAD, v2_void_t> * arg1, BAT<v2_void_t, V2TYPE_TAIL> * arg2); \
extern template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * matchjoin(BAT<V2TYPE_HEAD, v2_oid_t> * arg1, BAT<v2_oid_t, V2TYPE_TAIL> * arg2); \
extern template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * matchjoin(BAT<V2TYPE_HEAD, v2_id_t> * arg1, BAT<v2_id_t, V2TYPE_TAIL> * arg2); \
extern template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * matchjoin(BAT<V2TYPE_HEAD, v2_size_t> * arg1, BAT<v2_size_t, V2TYPE_TAIL> * arg2); \
extern template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * matchjoin(BAT<V2TYPE_HEAD, v2_tinyint_t> * arg1, BAT<v2_tinyint_t, V2TYPE_TAIL> * arg2); \
extern template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * matchjoin(BAT<V2TYPE_HEAD, v2_shortint_t> * arg1, BAT<v2_shortint_t, V2TYPE_TAIL> * arg2); \
extern template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * matchjoin(BAT<V2TYPE_HEAD, v2_int_t> * arg1, BAT<v2_int_t, V2TYPE_TAIL> * arg2); \
extern template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * matchjoin(BAT<V2TYPE_HEAD, v2_bigint_t> * arg1, BAT<v2_bigint_t, V2TYPE_TAIL> * arg2); \
extern template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * matchjoin(BAT<V2TYPE_HEAD, v2_str_t> * arg1, BAT<v2_str_t, V2TYPE_TAIL> * arg2); \
extern template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * matchjoin(BAT<V2TYPE_HEAD, v2_resoid_t> * arg1, BAT<v2_restiny_t, V2TYPE_TAIL> * arg2); \
extern template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * matchjoin(BAT<V2TYPE_HEAD, v2_restiny_t> * arg1, BAT<v2_restiny_t, V2TYPE_TAIL> * arg2); \
extern template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * matchjoin(BAT<V2TYPE_HEAD, v2_resshort_t> * arg1, BAT<v2_resshort_t, V2TYPE_TAIL> * arg2); \
extern template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * matchjoin(BAT<V2TYPE_HEAD, v2_resint_t> * arg1, BAT<v2_resint_t, V2TYPE_TAIL> * arg2); \
extern template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * matchjoin(BAT<V2TYPE_HEAD, v2_resbigint_t> * arg1, BAT<v2_resbigint_t, V2TYPE_TAIL> * arg2); \
extern template BAT<typename V2TYPE_HEAD::v2_select_t, typename V2TYPE_TAIL::v2_select_t> * matchjoin(BAT<V2TYPE_HEAD, v2_resstr_t> * arg1, BAT<v2_resstr_t, V2TYPE_TAIL> * arg2);

#define V2_MATCHJOIN(V2TYPE) \
V2_MATCHJOIN_SUB(v2_void_t, V2TYPE) \
V2_MATCHJOIN_SUB(v2_oid_t, V2TYPE) \
V2_MATCHJOIN_SUB(v2_id_t, V2TYPE) \
V2_MATCHJOIN_SUB(v2_size_t, V2TYPE) \
V2_MATCHJOIN_SUB(v2_tinyint_t, V2TYPE) \
V2_MATCHJOIN_SUB(v2_shortint_t, V2TYPE) \
V2_MATCHJOIN_SUB(v2_int_t, V2TYPE) \
V2_MATCHJOIN_SUB(v2_bigint_t, V2TYPE) \
V2_MATCHJOIN_SUB(v2_str_t, V2TYPE) \
V2_MATCHJOIN_SUB(v2_restiny_t, V2TYPE) \
V2_MATCHJOIN_SUB(v2_resoid_t, V2TYPE) \
V2_MATCHJOIN_SUB(v2_resshort_t, V2TYPE) \
V2_MATCHJOIN_SUB(v2_resint_t, V2TYPE) \
V2_MATCHJOIN_SUB(v2_resbigint_t, V2TYPE) \
V2_MATCHJOIN_SUB(v2_resstr_t, V2TYPE) \
extern template BAT<v2_void_t, typename V2TYPE::v2_select_t> * fetchjoin(BAT<v2_void_t, v2_oid_t> * arg1, BAT<v2_void_t, V2TYPE> * arg2);

            V2_MATCHJOIN(v2_void_t)
            V2_MATCHJOIN(v2_oid_t)
            V2_MATCHJOIN(v2_id_t)
            V2_MATCHJOIN(v2_size_t)
            V2_MATCHJOIN(v2_tinyint_t)
            V2_MATCHJOIN(v2_shortint_t)
            V2_MATCHJOIN(v2_int_t)
            V2_MATCHJOIN(v2_bigint_t)
            V2_MATCHJOIN(v2_str_t)
            V2_MATCHJOIN(v2_resoid_t)
            V2_MATCHJOIN(v2_restiny_t)
            V2_MATCHJOIN(v2_resshort_t)
            V2_MATCHJOIN(v2_resint_t)
            V2_MATCHJOIN(v2_resbigint_t)
            V2_MATCHJOIN(v2_resstr_t)

#undef V2_HASHJOIN
#undef V2_MATCHJOIN_SUB

#define V2_GROUPBY_SUB(Head, Tail) \
extern template std::pair<BAT<v2_void_t, v2_oid_t> *, BAT<v2_void_t, v2_oid_t> *> groupby(BAT<Head, Tail> * bat); \
extern template std::pair<BAT<v2_void_t, v2_oid_t> *, BAT<v2_void_t, v2_oid_t> *> groupby(BAT<Head, Tail> * bat, BAT<v2_void_t, v2_oid_t> * grouping);

#define V2_GROUPBY(Tail) \
V2_GROUPBY_SUB(v2_void_t, Tail) \
V2_GROUPBY_SUB(v2_oid_t, Tail)

            V2_GROUPBY(v2_oid_t)
            V2_GROUPBY(v2_id_t)
            V2_GROUPBY(v2_size_t)
            V2_GROUPBY(v2_tinyint_t)
            V2_GROUPBY(v2_shortint_t)
            V2_GROUPBY(v2_int_t)
            V2_GROUPBY(v2_bigint_t)
            V2_GROUPBY(v2_str_t)
            V2_GROUPBY(v2_resoid_t)
            V2_GROUPBY(v2_restiny_t)
            V2_GROUPBY(v2_resshort_t)
            V2_GROUPBY(v2_resint_t)
            V2_GROUPBY(v2_resbigint_t)
            V2_GROUPBY(v2_resstr_t)

#undef V2_GROUPBY

#define V2_AGGREGATE_SUM(V2RESULT, V2TYPE) \
extern template typename V2RESULT::type_t scalar::aggregate_sum<V2RESULT, v2_oid_t, V2TYPE>(BAT<v2_oid_t, V2TYPE> * arg); \
extern template typename V2RESULT::type_t sse::aggregate_sum<V2RESULT, v2_oid_t, V2TYPE>(BAT<v2_oid_t, V2TYPE> * arg);

            V2_AGGREGATE_SUM(v2_tinyint_t, v2_tinyint_t)
            V2_AGGREGATE_SUM(v2_shortint_t, v2_tinyint_t)
            V2_AGGREGATE_SUM(v2_int_t, v2_tinyint_t)
            V2_AGGREGATE_SUM(v2_bigint_t, v2_tinyint_t)
            V2_AGGREGATE_SUM(v2_shortint_t, v2_shortint_t)
            V2_AGGREGATE_SUM(v2_int_t, v2_shortint_t)
            V2_AGGREGATE_SUM(v2_bigint_t, v2_shortint_t)
            V2_AGGREGATE_SUM(v2_int_t, v2_int_t)
            V2_AGGREGATE_SUM(v2_bigint_t, v2_int_t)
            V2_AGGREGATE_SUM(v2_bigint_t, v2_bigint_t)

#undef V2_AGGREGATE_SUM

#define V2_AGGREGATE_MUL_SUM(V2RESULT, V2TAIL1, V2TAIL2) \
extern template BAT<v2_void_t, V2RESULT> * scalar::aggregate_mul_sum<V2RESULT, v2_oid_t, V2TAIL1, v2_oid_t, V2TAIL2>(BAT<v2_oid_t, V2TAIL1> * arg1, BAT<v2_oid_t, V2TAIL2> * arg2, typename V2RESULT::type_t init); \
extern template BAT<v2_void_t, V2RESULT> * sse::aggregate_mul_sum<V2RESULT, v2_oid_t, V2TAIL1, v2_oid_t, V2TAIL2>(BAT<v2_oid_t, V2TAIL1> * arg1, BAT<v2_oid_t, V2TAIL2> * arg2, typename V2RESULT::type_t init);

            V2_AGGREGATE_MUL_SUM(v2_shortint_t, v2_tinyint_t, v2_tinyint_t)
            V2_AGGREGATE_MUL_SUM(v2_int_t, v2_tinyint_t, v2_tinyint_t)
            V2_AGGREGATE_MUL_SUM(v2_bigint_t, v2_tinyint_t, v2_tinyint_t)
            V2_AGGREGATE_MUL_SUM(v2_int_t, v2_tinyint_t, v2_shortint_t)
            V2_AGGREGATE_MUL_SUM(v2_int_t, v2_shortint_t, v2_tinyint_t)
            V2_AGGREGATE_MUL_SUM(v2_bigint_t, v2_tinyint_t, v2_shortint_t)
            V2_AGGREGATE_MUL_SUM(v2_bigint_t, v2_shortint_t, v2_tinyint_t)
            V2_AGGREGATE_MUL_SUM(v2_bigint_t, v2_tinyint_t, v2_int_t)
            V2_AGGREGATE_MUL_SUM(v2_bigint_t, v2_int_t, v2_tinyint_t)
            V2_AGGREGATE_MUL_SUM(v2_bigint_t, v2_shortint_t, v2_int_t)
            V2_AGGREGATE_MUL_SUM(v2_bigint_t, v2_int_t, v2_shortint_t)
            V2_AGGREGATE_MUL_SUM(v2_bigint_t, v2_int_t, v2_int_t)
            V2_AGGREGATE_MUL_SUM(v2_bigint_t, v2_bigint_t, v2_int_t)
            V2_AGGREGATE_MUL_SUM(v2_bigint_t, v2_int_t, v2_bigint_t)

#undef V2_AGGREGATE_MUL_SUM

#define V2_AGGREGATE_SUM_GROUPED(Result, Head, Tail) \
extern template BAT<v2_void_t, Result> * aggregate_sum_grouped(BAT<Head, Tail> * bat, BAT<v2_void_t, v2_oid_t> * grouping, size_t numGroups);

            V2_AGGREGATE_SUM_GROUPED(v2_bigint_t, v2_oid_t, v2_int_t)

#undef V2_AGGREGATE_SUM_GROUPED

        }
    }
}

#endif /* OPERATORS_HPP */
