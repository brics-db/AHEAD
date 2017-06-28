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
 * File:   selectAN_seq.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 22. November 2016, 16:22
 *
 * File:   selectAN_scalar.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Renamed on 28. June 2017, 09:39
 */

#ifndef SELECT_AN_SEQ_TCC
#define SELECT_AN_SEQ_TCC

#include <type_traits>

#include <column_storage/Storage.hpp>
#include <column_operators/ANbase.hpp>
#include "../miscellaneous.hpp"

namespace ahead {
    namespace bat {
        namespace ops {
            namespace scalar {
                namespace Private {

                    template<template<typename > class Op, typename Head, typename Tail, bool reencode>
                    struct SelectionAN1 {
                    };

                    template<template<typename > class Op, typename Tail, bool reencode>
                    struct SelectionAN1<Op, v2_void_t, Tail, reencode> {

                        typedef v2_void_t Head;
                        typedef typename Head::type_t head_t;
                        typedef typename Tail::type_t tail_t;
                        typedef typename TypeMap<Head>::v2_encoded_t::v2_select_t v2_head_select_t;
                        typedef typename v2_head_select_t::type_t head_select_t;
                        typedef typename Tail::v2_select_t v2_tail_select_t;
                        typedef typename v2_tail_select_t::type_t tail_select_t;
                        typedef typename std::pair<BAT<v2_head_select_t, v2_tail_select_t>*, AN_indicator_vector*> result_t;

                        static result_t filter(
                                BAT<Head, Tail>* arg,
                                typename Tail::type_t threshold,
                                tail_select_t ATR = 1,
                                tail_select_t ATInvR = 1) {
                            static_assert(std::is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                            static_assert(std::is_base_of<v2_anencoded_t, Tail>::value, "ResTail must be an AN-encoded type");

                            // always encode head (void -> resoid)
                            const head_select_t AHead = std::get<v2_head_select_t::As->size() - 1>(*v2_head_select_t::As);
                            const head_select_t AHeadInv = std::get<v2_head_select_t::Ainvs->size() - 1>(*v2_head_select_t::Ainvs);
                            const tail_select_t ATailInv = static_cast<tail_select_t>(arg->tail.metaData.AN_Ainv);
                            const tail_select_t TailUnencMaxU = static_cast<tail_select_t>(arg->tail.metaData.AN_unencMaxU);
                            const resoid_t Aoid = std::get<15>(*v2_resoid_t::As);
                            auto result = std::make_pair(ahead::bat::ops::skeletonTail<v2_head_select_t, v2_tail_select_t>(arg), new AN_indicator_vector);
                            result.first->head.metaData = ColumnMetaData(sizeof(head_select_t), AHead, AHeadInv, v2_head_select_t::UNENC_MAX_U, v2_head_select_t::UNENC_MIN);
                            result.first->reserve(arg->size());
                            if (reencode) {
                                result.first->tail.metaData.AN_A = ATR;
                                result.first->tail.metaData.AN_Ainv = ATInvR;
                            }
                            result.second->reserve(32);
                            tail_select_t Areenc = reencode ? (ATailInv * ATR) : 1;
                            auto iter = arg->begin();
                            Op<tail_select_t> op;
                            for (size_t pos = 0; iter->hasNext(); ++*iter, ++pos) {
                                auto t = iter->tail();
                                if (static_cast<tail_t>(t * ATailInv) > TailUnencMaxU) {
                                    result.second->push_back(pos * Aoid);
                                }
                                if (op(t, threshold)) {
                                    if (reencode) {
                                        t *= Areenc;
                                    }
                                    result.first->append(std::make_pair(iter->head() * AHead, t));
                                }
                            }
                            delete iter;
                            return result;
                        }
                    };

                    template<template<typename > class Op>
                    struct SelectionAN1<Op, v2_void_t, v2_str_t, false> {

                        typedef v2_void_t Head;
                        typedef typename Head::type_t head_t;
                        typedef typename TypeMap<Head>::v2_encoded_t::v2_select_t v2_head_select_t;
                        typedef typename v2_head_select_t::type_t head_select_t;
                        typedef typename v2_str_t::v2_select_t v2_tail_select_t;
                        typedef typename std::pair<BAT<v2_head_select_t, v2_tail_select_t>*, AN_indicator_vector*> result_t;

                        static result_t filter(
                                BAT<Head, v2_str_t> * arg,
                                str_t threshold,
                                __attribute__ ((unused)) str_t ATR = nullptr,
                                __attribute__ ((unused)) str_t ATInvR = nullptr) {
                            static_assert(std::is_base_of<v2_base_t, Head>::value, "Head must be a base type");

                            // always encode head (void -> resoid)
                            const head_select_t AHead = std::get<v2_head_select_t::As->size() - 1>(*v2_head_select_t::As);
                            const head_select_t AHeadInv = std::get<v2_head_select_t::Ainvs->size() - 1>(*v2_head_select_t::Ainvs);
                            auto result = std::make_pair(ahead::bat::ops::skeletonTail<v2_head_select_t, v2_str_t>(arg), nullptr);
                            result.first->head.metaData = ColumnMetaData(sizeof(head_select_t), AHead, AHeadInv, v2_head_select_t::UNENC_MAX_U, v2_head_select_t::UNENC_MIN);
                            result.first->reserve(arg->size());
                            auto iter = arg->begin();
                            Op<int> op;
                            for (; iter->hasNext(); ++*iter) {
                                auto t = iter->tail();
                                if (op(strcmp(t, threshold), 0)) {
                                    result.first->append(std::make_pair(iter->head() * AHead, t));
                                }
                            }
                            delete iter;
                            return result;
                        }
                    };

                    template<template<typename > class Op1, template<typename > class Op2, template<typename > class OpCombine, typename Head, typename Tail, bool reencode>
                    struct SelectionAN2 {
                    };

                    template<template<typename > class Op1, template<typename > class Op2, template<typename > class OpCombine, typename Tail, bool reencode>
                    struct SelectionAN2<Op1, Op2, OpCombine, v2_void_t, Tail, reencode> {

                        typedef v2_void_t Head;
                        typedef typename Head::type_t head_t;
                        typedef typename Tail::type_t tail_t;
                        typedef typename TypeMap<Head>::v2_encoded_t::v2_select_t v2_head_select_t;
                        typedef typename v2_head_select_t::type_t head_select_t;
                        typedef typename Tail::v2_select_t v2_tail_select_t;
                        typedef typename v2_tail_select_t::type_t tail_select_t;
                        typedef typename std::pair<BAT<v2_head_select_t, v2_tail_select_t>*, AN_indicator_vector*> result_t;

                        static result_t filter(
                                BAT<Head, Tail> * arg,
                                tail_t threshold1,
                                tail_t threshold2,
                                tail_select_t ATR = 1,
                                tail_select_t ATInvR = 1) {
                            static_assert(std::is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                            static_assert(std::is_base_of<v2_anencoded_t, Tail>::value, "ResTail must be an AN-encoded type");
                            static_assert(std::is_base_of<ahead::bat::ops::functor, OpCombine<void>>::value, "OpCombine template parameter must be a functor (see include/column_operators/functors.hpp)");

                            // always encode head (void -> resoid)
                            const head_select_t AHead = std::get<v2_head_select_t::As->size() - 1>(*v2_head_select_t::As);
                            const head_select_t AHeadInv = std::get<v2_head_select_t::Ainvs->size() - 1>(*v2_head_select_t::Ainvs);
                            const tail_select_t ATailInv = static_cast<tail_select_t>(arg->tail.metaData.AN_Ainv);
                            const tail_select_t TailUnencMaxU = static_cast<tail_select_t>(arg->tail.metaData.AN_unencMaxU);
                            const resoid_t Aoid = std::get<15>(*v2_resoid_t::As);
                            auto result = std::make_pair(ahead::bat::ops::skeletonTail<v2_head_select_t, v2_tail_select_t>(arg), new AN_indicator_vector);
                            result.first->head.metaData = ColumnMetaData(sizeof(head_select_t), AHead, AHeadInv, v2_head_select_t::UNENC_MAX_U, v2_head_select_t::UNENC_MIN);
                            result.first->reserve(arg->size());
                            if (reencode) {
                                result.first->tail.metaData.AN_A = ATR;
                                result.first->tail.metaData.AN_Ainv = ATInvR;
                            }
                            result.second->reserve(32);
                            tail_select_t Areenc = reencode ? (ATailInv * ATR) : 1;
                            auto iter = arg->begin();
                            Op1<tail_select_t> op1;
                            Op2<tail_select_t> op2;
                            for (size_t pos = 0; iter->hasNext(); ++*iter, ++pos) {
                                auto t = iter->tail();
                                if (static_cast<tail_t>(t * ATailInv) > TailUnencMaxU) {
                                    result.second->push_back(pos * Aoid);
                                }
                                if (OpCombine<void>()(op1(t, std::forward<tail_select_t>(threshold1)), op2(t, std::forward<tail_select_t>(threshold2)))) {
                                    if (reencode) {
                                        t *= Areenc;
                                    }
                                    result.first->append(std::make_pair(iter->head() * AHead, t));
                                }
                            }
                            delete iter;
                            return result;
                        }
                    };

                    template<template<typename > class Op1, template<typename > class Op2, template<typename > class OpCombine>
                    struct SelectionAN2<Op1, Op2, OpCombine, v2_void_t, v2_str_t, false> {

                        typedef v2_void_t Head;
                        typedef typename Head::type_t head_t;
                        typedef typename TypeMap<Head>::v2_encoded_t::v2_select_t v2_head_select_t;
                        typedef typename v2_head_select_t::type_t head_select_t;
                        typedef typename v2_str_t::v2_select_t v2_tail_select_t;
                        typedef typename v2_tail_select_t::type_t tail_select_t;
                        typedef typename std::pair<BAT<v2_head_select_t, v2_tail_select_t>*, AN_indicator_vector*> result_t;

                        static result_t filter(
                                BAT<Head, v2_str_t> * arg,
                                tail_select_t threshold1,
                                tail_select_t threshold2,
                                str_t ATR = nullptr,
                                str_t ATInvR = nullptr) {
                            static_assert(std::is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                            static_assert(std::is_base_of<ahead::bat::ops::functor, OpCombine<void>>::value, "OpCombine template parameter must be a functor (see include/column_operators/functors.hpp)");

                            // always encode head (void -> resoid)
                            (void) ATR;
                            (void) ATInvR;
                            const head_select_t AHead = std::get<v2_head_select_t::As->size() - 1>(*v2_head_select_t::As);
                            const head_select_t AHeadInv = std::get<v2_head_select_t::Ainvs->size() - 1>(*v2_head_select_t::Ainvs);
                            auto result = std::make_pair(ahead::bat::ops::skeletonTail<v2_head_select_t, v2_str_t>(arg), nullptr);
                            result.first->head.metaData = ColumnMetaData(sizeof(head_select_t), AHead, AHeadInv, v2_head_select_t::UNENC_MAX_U, v2_head_select_t::UNENC_MIN);
                            result.first->reserve(arg->size());
                            auto iter = arg->begin();
                            Op1<int> op1;
                            Op2<int> op2;
                            for (; iter->hasNext(); ++*iter) {
                                auto t = iter->tail();
                                if (OpCombine<void>()(op1(strcmp(t, std::forward<tail_select_t>(threshold1)), 0), op2(strcmp(t, std::forward<tail_select_t>(threshold2)), 0))) {
                                    result.first->append(std::make_pair(iter->head() * AHead, t));
                                }
                            }
                            delete iter;
                            return result;
                        }
                    };

                }
            }
        }
    }
}

#endif /* SELECT_AN_SEQ_TCC */
