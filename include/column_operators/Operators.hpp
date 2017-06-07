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
            groupby(BAT<Head, Tail> * bat, BAT<v2_void_t, v2_oid_t> * grouping, size_t numGroups);

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

        }
    }
}

#endif /* OPERATORS_HPP */
