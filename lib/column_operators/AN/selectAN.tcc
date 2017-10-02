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
 * File:   selectAN.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 22. November 2016, 16:22
 */

#ifndef SELECT_AN_TCC
#define SELECT_AN_TCC

#include <type_traits>

#include <column_storage/Storage.hpp>
#include <column_operators/ANbase.hpp>
#include "selectAN_SSE.tcc"
#include "selectAN_scalar.tcc"

namespace ahead {
    namespace bat {
        namespace ops {

            namespace scalar {
                template<template<typename > class Op, typename Head, typename Tail>
                std::pair<BAT<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t> *, AN_indicator_vector *> selectAN(
                        BAT<Head, Tail> * arg,
                        typename Tail::type_t threshold,
                        resoid_t AOID) {
                    return Private::SelectionAN1<Op, Head, Tail, false>::filter(arg, threshold, AOID);
                }

                template<template<typename > class Op1, template<typename > class Op2, template<typename > class OpCombine, typename Head, typename Tail>
                std::pair<BAT<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t> *, AN_indicator_vector *> selectAN(
                        BAT<Head, Tail> * arg,
                        typename Tail::type_t threshold1,
                        typename Tail::type_t threshold2,
                        resoid_t AOID) {
                    return Private::SelectionAN2<Op1, Op2, OpCombine, Head, Tail, false>::filter(arg, threshold1, threshold2, AOID);
                }

                template<template<typename > class Op, typename Head, typename Tail>
                std::pair<BAT<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t> *, AN_indicator_vector *> selectAN(
                        BAT<Head, Tail> * arg,
                        typename Tail::type_t threshold,
                        typename Tail::v2_select_t::type_t ATR,
                        typename Tail::v2_select_t::type_t ATInvR,
                        resoid_t AOID) {
                    return Private::SelectionAN1<Op, Head, Tail, true>::filter(arg, threshold, AOID, ATR, ATInvR);
                }

                template<template<typename > class Op1, template<typename > class Op2, template<typename > class OpCombine, typename Head, typename Tail>
                std::pair<BAT<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t> *, AN_indicator_vector *> selectAN(
                        BAT<Head, Tail> * arg,
                        typename Tail::type_t threshold1,
                        typename Tail::type_t threshold2,
                        typename Tail::v2_select_t::type_t ATR,
                        typename Tail::v2_select_t::type_t ATInvR,
                        resoid_t AOID) {
                    return Private::SelectionAN2<Op1, Op2, OpCombine, Head, Tail, true>::filter(arg, threshold1, threshold2, AOID, ATR, ATInvR);
                }
            }

            namespace simd {
                namespace sse {

                    template<template<typename > class Op, typename Head, typename Tail>
                    std::pair<BAT<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t> *, AN_indicator_vector *> selectAN(
                            BAT<Head, Tail> * arg,
                            typename Tail::type_t threshold,
                            resoid_t AOID) {
                        return Private::SelectionAN1<Op, Head, Tail, false>::filter(arg, threshold, AOID);
                    }

                    template<template<typename > class Op1, template<typename > class Op2, template<typename > class OpCombine, typename Head, typename Tail>
                    std::pair<BAT<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t> *, AN_indicator_vector *> selectAN(
                            BAT<Head, Tail> * arg,
                            typename Tail::type_t threshold1,
                            typename Tail::type_t threshold2,
                            resoid_t AOID) {
                        return Private::SelectionAN2<Op1, Op2, OpCombine, Head, Tail, false>::filter(arg, threshold1, threshold2, AOID);
                    }

                    template<template<typename > class Op, typename Head, typename Tail>
                    std::pair<BAT<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t> *, AN_indicator_vector *> selectAN(
                            BAT<Head, Tail> * arg,
                            typename Tail::type_t threshold,
                            typename Tail::v2_select_t::type_t ATR,
                            typename Tail::v2_select_t::type_t ATInvR,
                            resoid_t AOID) {
                        return Private::SelectionAN1<Op, Head, Tail, true>::filter(arg, threshold, AOID, ATR, ATInvR);
                    }

                    template<template<typename > class Op1, template<typename > class Op2, template<typename > class OpCombine, typename Head, typename Tail>
                    std::pair<BAT<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t> *, AN_indicator_vector *> selectAN(
                            BAT<Head, Tail> * arg,
                            typename Tail::type_t threshold1,
                            typename Tail::type_t threshold2,
                            typename Tail::v2_select_t::type_t ATR,
                            typename Tail::v2_select_t::type_t ATInvR,
                            resoid_t AOID) {
                        return Private::SelectionAN2<Op1, Op2, OpCombine, Head, Tail, true>::filter(arg, threshold1, threshold2, AOID, ATR, ATInvR);
                    }

                }
            }

        }
    }
}

#endif /* SELECT_AN_TCC */
