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
 * File:   select_AN.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 22. November 2016, 16:22
 */

#ifndef SELECT_AN_TCC
#define SELECT_AN_TCC

#include <type_traits>

#include <column_storage/Storage.hpp>

#ifdef FORCE_SSE
#include "selectAN_SSE.tcc"
#else
#include "selectAN_seq.tcc"
#endif

namespace ahead {
    namespace bat {
        namespace ops {

            template<template<typename > class Op, typename Head, typename Tail>
            std::pair<BAT<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t>*, std::vector<bool>*> selectAN(BAT<Head, Tail>* arg, typename Tail::type_t&& threshold) {
                return Private::SelectionAN1<Op, Head, Tail, false>::filter(arg, std::forward<typename Tail::type_t>(threshold));
            }

            template<template<typename > class Op1 = std::greater_equal, template<typename > class Op2 = std::less_equal, typename Head, typename Tail>
            std::pair<BAT<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t>*, std::vector<bool>*> selectAN(BAT<Head, Tail>* arg, typename Tail::type_t&& threshold1,
                    typename Tail::type_t&& threshold2) {
                return Private::SelectionAN2<Op1, Op2, Head, Tail, false>::filter(arg, std::forward<typename Tail::type_t>(threshold1), std::forward<typename Tail::type_t>(threshold2));
            }

            template<template<typename > class Op, typename Head, typename Tail>
            std::pair<BAT<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t>*, std::vector<bool>*> selectAN(BAT<Head, Tail>* arg, typename Tail::type_t && threshold,
                    typename Tail::v2_select_t::type_t ATR, typename Tail::v2_select_t::type_t ATInvR) {
                return Private::SelectionAN1<Op, Head, Tail, true>::filter(arg, std::forward<typename Tail::type_t>(threshold), ATR, ATInvR);
            }

            template<template<typename > class Op1 = std::greater_equal, template<typename > class Op2 = std::less_equal, typename Head, typename Tail>
            std::pair<BAT<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t>*, std::vector<bool>*> selectAN(BAT<Head, Tail>* arg, typename Tail::type_t&& threshold1,
                    typename Tail::type_t&& threshold2, typename Tail::v2_select_t::type_t ATR, typename Tail::v2_select_t::type_t ATInvR) {
                return Private::SelectionAN2<Op1, Op2, Head, Tail, true>::filter(arg, std::forward<typename Tail::type_t>(threshold1), std::forward<typename Tail::type_t>(threshold2), ATR, ATInvR);
            }

        }
    }
}

#endif /* SELECT_AN_TCC */
