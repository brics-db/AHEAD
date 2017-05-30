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

/* 
 * File:   select.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 22. November 2016, 21:02
 */

#ifndef SELECT_TCC
#define SELECT_TCC

#include <utility>
#include <functional>

#include <column_storage/Storage.hpp>
#include "select_SSE.tcc"
#include "select_seq.tcc"

namespace ahead {
    namespace bat {
        namespace ops {

            namespace scalar {
                template<template<typename > class Op, typename Head, typename Tail>
                BAT<typename Head::v2_select_t, typename Tail::v2_select_t>*
                select(BAT<Head, Tail>* arg, typename Tail::type_t && th1) {
                    return Private::Selection1<Op, Head, Tail>::filter(arg, std::forward<typename Tail::type_t>(th1));
                }

                template<template<typename > class Op1 = std::greater_equal, template<typename > class Op2 = std::less_equal, typename Head, typename Tail>
                BAT<typename Head::v2_select_t, typename Tail::v2_select_t>*
                select(BAT<Head, Tail>* arg, typename Tail::type_t && th1, typename Tail::type_t && th2) {
                    return Private::Selection2<Op1, Op2, Head, Tail>::filter(arg, std::forward<typename Tail::type_t>(th1), std::forward<typename Tail::type_t>(th2));
                }
            }

            namespace sse {
                template<template<typename > class Op, typename Head, typename Tail>
                BAT<typename Head::v2_select_t, typename Tail::v2_select_t>*
                select(BAT<Head, Tail>* arg, typename Tail::type_t && th1) {
                    return Private::Selection1<Op, Head, Tail>::filter(arg, std::forward<typename Tail::type_t>(th1));
                }

                template<template<typename > class Op1 = std::greater_equal, template<typename > class Op2 = std::less_equal, typename Head, typename Tail>
                BAT<typename Head::v2_select_t, typename Tail::v2_select_t>*
                select(BAT<Head, Tail>* arg, typename Tail::type_t && th1, typename Tail::type_t && th2) {
                    return Private::Selection2<Op1, Op2, Head, Tail>::filter(arg, std::forward<typename Tail::type_t>(th1), std::forward<typename Tail::type_t>(th2));
                }
            }

        }
    }
}

#endif /* SELECT_TCC */
