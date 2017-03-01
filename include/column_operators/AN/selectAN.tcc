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
 * Created on 22. November 2016, 16:22
 */

#ifndef SELECT_AN_TCC
#define SELECT_AN_TCC

#include <type_traits>

#include <ColumnStore.h>
#include <column_storage/Bat.h>
#include <column_storage/TempBat.h>

namespace v2 {
    namespace bat {
        namespace ops {

            namespace Private {

                template<typename Op, typename Head, typename Tail, bool reencode>
                struct SelectionAN1 {

                    typedef typename Head::type_t head_t;
                    typedef typename Tail::type_t tail_t;
                    typedef typename TypeMap<Head>::v2_encoded_t::v2_select_t v2_head_select_t;
                    typedef typename v2_head_select_t::type_t head_select_t;
                    typedef typename Tail::v2_select_t v2_tail_select_t;
                    typedef typename v2_tail_select_t::type_t tail_select_t;

                    std::pair<TempBAT<v2_head_select_t, v2_tail_select_t>*, std::vector<bool>*> operator()(BAT<Head, Tail>* arg, typename Tail::type_t&& threshold, tail_select_t ATR = 1, // for reencoding
                            tail_select_t ATInvR = 1 // for reencoding
                            ) {
                        // TODO for now we assume that selection is only done on base BATs!!! Of course, there could be selections on BATs with encoded heads!
                        static_assert(std::is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                        static_assert(std::is_base_of<v2_anencoded_t, Tail>::value, "ResTail must be an AN-encoded type");

                        // always encode head (will usually be a conversion from void -> oid)
                        const head_select_t AHead = std::get<v2_head_select_t::As->size() - 1>(*v2_head_select_t::As);
                        const head_select_t AHeadInv = std::get<v2_head_select_t::Ainvs->size() - 1>(*v2_head_select_t::Ainvs);
                        const tail_select_t ATailInv = static_cast<tail_t>(arg->tail.metaData.AN_Ainv);
                        const tail_select_t TailUnencMaxU = static_cast<tail_t>(arg->tail.metaData.AN_unencMaxU);
                        auto result = std::make_pair(v2::bat::ops::skeletonTail<v2_head_select_t, v2_tail_select_t>(arg), new std::vector<bool>(arg->size()));
                        result.first->head.metaData = ColumnMetaData(sizeof(head_select_t), AHead, AHeadInv, v2_head_select_t::UNENC_MAX_U, v2_head_select_t::UNENC_MIN);
                        result.first->reserve(arg->size() / 2);
                        if (reencode) {
                            result.first->tail.metaData.AN_A = ATR;
                            result.first->tail.metaData.AN_Ainv = ATInvR;
                        }
                        tail_select_t Areenc = reencode ? (ATailInv * ATR) : 1;
                        auto iter = arg->begin();
                        Op op;
                        for (size_t i = 0; iter->hasNext(); ++*iter, ++i) {
                            auto t = iter->tail();
                            if ((t * ATailInv) <= TailUnencMaxU) {
                                (*result.second)[i] = true;
                            }
                            if (op(t, threshold)) {
                                if (reencode)
                                    t *= Areenc;
                                result.first->append(std::make_pair(iter->head() * AHead, t));
                            }
                        }
                        delete iter;
                        return result;
                    }
                };

                template<typename Op, typename Head>
                struct SelectionAN1<Op, Head, v2_str_t, false> {

                    typedef typename Head::type_t head_t;
                    typedef typename TypeMap<Head>::v2_encoded_t::v2_select_t v2_head_select_t;
                    typedef typename v2_head_select_t::type_t head_select_t;
                    typedef typename v2_str_t::v2_select_t v2_tail_select_t;

                    std::pair<TempBAT<v2_head_select_t, v2_tail_select_t>*, std::vector<bool>*> operator()(BAT<Head, v2_str_t> * arg, str_t && threshold, __attribute__ ((unused)) str_t ATR = nullptr, // cuurently only to match the signature
                            __attribute__ ((unused)) str_t ATInvR = nullptr // cuurently only to match the signature
                            ) {
                        // TODO for now we assume that selection is only done on base BATs!!! Of course, there could be selections on BATs with encoded heads!
                        static_assert(std::is_base_of<v2_base_t, Head>::value, "Head must be a base type");

                        // always encode head (will usually be a conversion from void -> oid)
                        const head_select_t AHead = std::get<v2_head_select_t::As->size() - 1>(*v2_head_select_t::As);
                        const head_select_t AHeadInv = std::get<v2_head_select_t::Ainvs->size() - 1>(*v2_head_select_t::Ainvs);
                        auto result = std::make_pair(v2::bat::ops::skeletonTail<v2_head_select_t, v2_str_t>(arg), nullptr);
                        result.first->head.metaData = ColumnMetaData(sizeof(head_select_t), AHead, AHeadInv, v2_head_select_t::UNENC_MAX_U, v2_head_select_t::UNENC_MIN);
                        result.first->reserve(arg->size() / 2);
                        auto iter = arg->begin();
                        Op op;
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

                template<typename Op1, typename Op2, typename Head, typename Tail, bool reencode>
                struct SelectionAN2 {

                    typedef typename Head::type_t head_t;
                    typedef typename Tail::type_t tail_t;
                    typedef typename TypeMap<Head>::v2_encoded_t::v2_select_t v2_head_select_t;
                    typedef typename v2_head_select_t::type_t head_select_t;
                    typedef typename Tail::v2_select_t v2_tail_select_t;
                    typedef typename v2_tail_select_t::type_t tail_select_t;

                    std::pair<TempBAT<v2_head_select_t, v2_tail_select_t>*, std::vector<bool>*> operator()(BAT<Head, Tail> * arg, tail_t && threshold1, tail_t && threshold2, tail_select_t ATR = 1, // for reencoding
                            tail_select_t ATInvR = 1 // for reencoding
                            ) {
                        // TODO for now we assume that selection is only done on base BATs!!! Of course, there could be selections on BATs with encoded heads!
                        static_assert(std::is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                        static_assert(std::is_base_of<v2_anencoded_t, Tail>::value, "ResTail must be an AN-encoded type");

                        // always encode head (will usually be a conversion from void -> oid)
                        const head_select_t AHead = std::get<v2_head_select_t::As->size() - 1>(*v2_head_select_t::As);
                        const head_select_t AHeadInv = std::get<v2_head_select_t::Ainvs->size() - 1>(*v2_head_select_t::Ainvs);
                        const tail_select_t ATailInv = static_cast<tail_select_t>(arg->tail.metaData.AN_Ainv);
                        const tail_select_t TailUnencMaxU = static_cast<tail_select_t>(arg->tail.metaData.AN_unencMaxU);
                        auto result = std::make_pair(v2::bat::ops::skeletonTail<v2_head_select_t, v2_tail_select_t>(arg), new std::vector<bool>(arg->size()));
                        result.first->head.metaData = ColumnMetaData(sizeof(head_select_t), AHead, AHeadInv, v2_head_select_t::UNENC_MAX_U, v2_head_select_t::UNENC_MIN);
                        result.first->reserve(arg->size() / 2);
                        if (reencode) {
                            result.first->tail.metaData.AN_A = ATR;
                            result.first->tail.metaData.AN_Ainv = ATInvR;
                        }
                        tail_select_t Areenc = reencode ? (ATailInv * ATR) : 1;
                        auto iter = arg->begin();
                        Op1 op1;
                        Op2 op2;
                        for (size_t i = 0; iter->hasNext(); ++*iter, ++i) {
                            auto t = iter->tail();
                            if ((t * ATailInv) <= TailUnencMaxU) {
                                (*result.second)[i] = true;
                            }
                            if (op1(t, threshold1) && op2(t, threshold2)) {
                                if (reencode)
                                    t *= Areenc;
                                result.first->append(std::make_pair(iter->head() * AHead, t));
                            }
                        }
                        delete iter;
                        return result;
                    }
                };

                template<typename Op1, typename Op2, typename Head>
                struct SelectionAN2<Op1, Op2, Head, v2_str_t, false> {

                    typedef typename Head::type_t head_t;
                    typedef typename TypeMap<Head>::v2_encoded_t::v2_select_t v2_head_select_t;
                    typedef typename v2_head_select_t::type_t head_select_t;
                    typedef v2_str_t v2_tail_select_t;

                    std::pair<TempBAT<v2_head_select_t, v2_tail_select_t>*, std::vector<bool>*> operator()(BAT<Head, v2_str_t> * arg, str_t&& threshold1, str_t&& threshold2, str_t ATR = nullptr, // cuurently only to match the signature
                            str_t ATInvR = nullptr // cuurently only to match the signature
                            ) {
                        // TODO for now we assume that selection is only done on base BATs!!! Of course, there could be selections on BATs with encoded heads!
                        static_assert(std::is_base_of<v2_base_t, Head>::value, "Head must be a base type");

                        // always encode head (will usually be a conversion from void -> oid)
                        const head_select_t AHead = std::get<v2_head_select_t::As->size() - 1>(*v2_head_select_t::As);
                        const head_select_t AHeadInv = std::get<v2_head_select_t::Ainvs->size() - 1>(*v2_head_select_t::Ainvs);
                        auto result = std::make_pair(v2::bat::ops::skeletonTail<v2_head_select_t, v2_str_t>(arg), nullptr);
                        result.first->head.metaData = ColumnMetaData(sizeof(head_select_t), AHead, AHeadInv, v2_head_select_t::UNENC_MAX_U, v2_head_select_t::UNENC_MIN);
                        result.first->reserve(arg->size() / 2);
                        auto iter = arg->begin();
                        Op1 op1;
                        Op2 op2;
                        for (; iter->hasNext(); ++*iter) {
                            auto t = iter->tail();
                            if (op1(strcmp(t, threshold1), 0) && op2(strcmp(t, threshold2), 0)) {
                                result.first->append(std::make_pair(iter->head() * AHead, t));
                            }
                        }
                        delete iter;
                        return result;
                    }
                };
            }

            template<template<typename > class Op, typename Head, typename Tail>
            std::pair<TempBAT<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t>*, std::vector<bool>*> selectAN(TempBAT<Head, Tail>* arg,
                    typename Tail::type_t&& threshold) {
                typedef typename Tail::v2_compare_t::type_t v2_compare_t;
                typedef typename Tail::type_t tail_t;
                Private::SelectionAN1<Op<v2_compare_t>, Head, Tail, false> impl;
                return impl(arg, std::forward<tail_t>(threshold));
            }

            template<template<typename > class Op1 = std::greater_equal, template<typename > class Op2 = std::less_equal, typename Head, typename Tail>
            std::pair<TempBAT<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t>*, std::vector<bool>*> selectAN(TempBAT<Head, Tail>* arg,
                    typename Tail::type_t&& threshold1, typename Tail::type_t&& threshold2) {
                typedef typename Tail::v2_compare_t::type_t v2_compare_t;
                typedef typename Tail::type_t tail_t;
                Private::SelectionAN2<Op1<v2_compare_t>, Op2<v2_compare_t>, Head, Tail, false> impl;
                return impl(arg, std::forward<tail_t>(threshold1), std::forward<tail_t>(threshold2));
            }

            template<template<typename > class Op, typename Head, typename Tail>
            std::pair<TempBAT<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t>*, std::vector<bool>*> selectAN(TempBAT<Head, Tail>* arg,
                    typename Tail::type_t && threshold, typename Tail::v2_select_t::type_t ATR, typename Tail::v2_select_t::type_t ATInvR) {
                typedef typename Tail::v2_compare_t::type_t v2_compare_t;
                typedef typename Tail::type_t tail_t;
                Private::SelectionAN1<Op<v2_compare_t>, Head, Tail, true> impl;
                return impl(arg, std::forward<tail_t>(threshold), ATR, ATInvR);
            }

            template<template<typename > class Op1 = std::greater_equal, template<typename > class Op2 = std::less_equal, typename Head, typename Tail>
            std::pair<TempBAT<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t>*, std::vector<bool>*> selectAN(TempBAT<Head, Tail>* arg,
                    typename Tail::type_t&& threshold1, typename Tail::type_t&& threshold2, typename Tail::v2_select_t::type_t ATR, typename Tail::v2_select_t::type_t ATInvR) {
                typedef typename Tail::v2_compare_t::type_t v2_compare_t;
                typedef typename Tail::type_t tail_t;
                Private::SelectionAN2<Op1<v2_compare_t>, Op2<v2_compare_t>, Head, Tail, true> impl;
                return impl(arg, std::forward<tail_t>(threshold1), std::forward<tail_t>(threshold2), ATR, ATInvR);
            }
        }
    }
}

#endif /* SELECT_AN_TCC */
