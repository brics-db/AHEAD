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
 * File:   selectAN_SSE.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 22. November 2016, 16:22
 */

#ifndef SELECT_AN_SSE_TCC
#define SELECT_AN_SSE_TCC

#include <type_traits>

#include <column_storage/Storage.hpp>
#include <column_operators/ANbase.hpp>
#include "SSEAN.hpp"
#include "../miscellaneous.hpp"

namespace ahead {
    namespace bat {
        namespace ops {
            namespace sse {
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
                        typedef typename v2_mm128_cmp<tail_t, Op>::mask_t tail_mask_t;
                        typedef typename std::pair<BAT<v2_head_select_t, v2_tail_select_t>*, AN_indicator_vector*> result_t;

                        static result_t filter(BAT<Head, Tail>* arg, typename Tail::type_t&& th, tail_select_t ATR = 1, // for reencoding
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
                            const resoid_t Aoid = std::get<15>(*v2_resoid_t::As);
                            auto result = std::make_pair(ahead::bat::ops::skeletonTail<v2_head_select_t, v2_tail_select_t>(arg), new AN_indicator_vector);
                            result.first->head.metaData = ColumnMetaData(sizeof(head_select_t), AHead, AHeadInv, v2_head_select_t::UNENC_MAX_U, v2_head_select_t::UNENC_MIN);
                            result.first->reserve(arg->size() / 2);
                            if (reencode) {
                                result.first->tail.metaData.AN_A = ATR;
                                result.first->tail.metaData.AN_Ainv = ATInvR;
                            }
                            result.second->reserve(32);
                            tail_select_t Areenc = reencode ? (ATailInv * ATR) : 1;

                            auto mmATInv = v2_mm128<tail_select_t>::set1(ATailInv);
                            auto mmATReenc = v2_mm128<tail_select_t>::set1(Areenc);
                            auto mmDMax = v2_mm128<tail_select_t>::set1(TailUnencMaxU);
                            auto mmThreshold = v2_mm128<tail_t>::set1(th);
                            auto szTail = arg->tail.container->size();
                            auto pT = arg->tail.container->data();
                            auto pTEnd = pT + szTail;
                            auto pmmT = reinterpret_cast<__m128i *>(pT);
                            auto pmmTEnd = reinterpret_cast<__m128i *>(pTEnd);
                            auto pRH = reinterpret_cast<head_select_t*>(result.first->head.container->data());
                            auto pRT = reinterpret_cast<tail_select_t*>(result.first->tail.container->data());
                            // we encode the OIDs here already and increase accordingly by multiples of AHead
                            auto mmOID = v2_mm128<head_select_t>::set_inc(arg->head.metaData.seqbase * AHead, AHead); // fill the vector with increasing values starting at seqbase

                            constexpr const size_t headsPerMM128 = sizeof(__m128i) / sizeof (head_select_t);
                            constexpr const size_t tailsPerMM128 = sizeof(__m128i) / sizeof (tail_select_t);

                            size_t pos = 0;
                            if (larger_type<head_select_t, tail_t>::isFirstLarger) {
                                constexpr const size_t factor = sizeof(head_select_t) / sizeof(tail_t);
                                constexpr const tail_mask_t maskMask = static_cast<tail_mask_t>((1ull << headsPerMM128) - 1);
                                auto mmInc = v2_mm128<head_select_t>::set1((sizeof(__m128i) / sizeof (head_select_t)) * AHead);
                                for (; pmmT <= (pmmTEnd - 1); ++pmmT) {
                                    auto mm = _mm_lddqu_si128(pmmT);
                                    v2_mm128_AN<tail_t>::detect(mm, mmATInv, mmDMax, result.second, pos, Aoid); // we only need to check the tail types since the head is virtual anyways
                                    // comparison on encoded values
                                    auto mask = v2_mm128_cmp<tail_t, Op>::cmp_mask(mm, mmThreshold);
                                    if (mask) {
                                        auto maskTmp = mask;
                                        for (size_t i = 0; i < factor; ++i) {
                                            auto actMask = maskTmp & maskMask;
                                            if (actMask) {
                                                v2_mm128<head_select_t>::pack_right2(pRH, mmOID, actMask);
                                            }
                                            mmOID = v2_mm128<head_select_t>::add(mmOID, mmInc);
                                            maskTmp >>= headsPerMM128;
                                        }
                                        pos += tailsPerMM128;
                                        if (reencode) {
                                            mm = v2_mm128<tail_t>::mullo(mm, mmATReenc);
                                        }
                                        v2_mm128<tail_t>::pack_right2(pRT, mm, mask);
                                    } else {
                                        mmOID = v2_mm128<head_select_t>::add(mmOID, v2_mm128<head_select_t>::mullo(mmInc, v2_mm128<head_select_t>::set1(factor)));
                                    }
                                }
                            } else {
                                // currently, resoid_t is the largest type anyways, so the tail type will be at most as large as resoid_t
                                auto mmInc = v2_mm128<head_select_t>::set1((sizeof(__m128i) / sizeof (tail_select_t)) * AHead);
                                for (; pmmT <= (pmmTEnd - 1); ++pmmT) {
                                    auto mm = _mm_lddqu_si128(pmmT);
                                    v2_mm128_AN<tail_t>::detect(mm, mmATInv, mmDMax, result.second, pos, Aoid); // we only need to check the tail types since the head is virtual anyways
                                    // comparison on encoded values
                                    auto mask = v2_mm128_cmp<tail_t, Op>::cmp_mask(mm, mmThreshold);
                                    if (mask) {
                                        // the mask will select only the lower OIDs anyways
                                        v2_mm128<head_select_t>::pack_right2(pRH, mmOID, mask);
                                        pos += tailsPerMM128;
                                        if (reencode) {
                                            mm = v2_mm128<tail_t>::mullo(mm, mmATReenc);
                                        }
                                        v2_mm128<tail_t>::pack_right2(pRT, mm, mask);
                                    }
                                    mmOID = v2_mm128<head_select_t>::add(mmOID, mmInc);
                                }
                            }
                            const size_t numSelectedValues = pRT - reinterpret_cast<tail_select_t*>(result.first->tail.container->data());
                            result.first->overwrite_size(numSelectedValues); // "register" the number of values we added
                            auto iter = arg->begin();
                            const size_t numFilteredValues = reinterpret_cast<tail_select_t*>(pmmT) - pT;
                            *iter += (numFilteredValues);
                            Op<tail_t> op;
                            for (; iter->hasNext(); ++*iter, ++pos) {
                                auto t = iter->tail();
                                if (static_cast<tail_select_t>(t * ATailInv) > TailUnencMaxU) {
                                    result.second->push_back(pos * Aoid);
                                }
                                if (op(t, th)) {
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

                        static result_t filter(BAT<Head, v2_str_t> * arg, str_t && threshold, __attribute__ ((unused)) str_t ATR = nullptr, __attribute__ ((unused)) str_t ATInvR = nullptr) {
                            // TODO for now we assume that selection is only done on base BATs!!! Of course, there could be selections on BATs with encoded heads!
                            static_assert(std::is_base_of<v2_base_t, Head>::value, "Head must be a base type");

                            // always encode head (will usually be a conversion from void -> oid)
                            const head_select_t AHead = std::get<v2_head_select_t::As->size() - 1>(*v2_head_select_t::As);
                            const head_select_t AHeadInv = std::get<v2_head_select_t::Ainvs->size() - 1>(*v2_head_select_t::Ainvs);
                            auto result = std::make_pair(ahead::bat::ops::skeletonTail<v2_head_select_t, v2_str_t>(arg), nullptr);
                            result.first->head.metaData = ColumnMetaData(sizeof(head_select_t), AHead, AHeadInv, v2_head_select_t::UNENC_MAX_U, v2_head_select_t::UNENC_MIN);
                            result.first->reserve(arg->size() / 2);
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

                    template<template<typename > class Op1, template<typename > class Op2, typename Head, typename Tail, bool reencode>
                    struct SelectionAN2 {
                    };

                    template<template<typename > class Op1, template<typename > class Op2, typename Tail, bool reencode>
                    struct SelectionAN2<Op1, Op2, v2_void_t, Tail, reencode> {

                        typedef v2_void_t Head;
                        typedef typename Head::type_t head_t;
                        typedef typename Tail::type_t tail_t;
                        typedef typename TypeMap<Head>::v2_encoded_t::v2_select_t v2_head_select_t;
                        typedef typename v2_head_select_t::type_t head_select_t;
                        typedef typename Tail::v2_select_t v2_tail_select_t;
                        typedef typename v2_tail_select_t::type_t tail_select_t;
                        typedef typename v2_mm128_cmp<tail_t, Op1>::mask_t tail_mask_t;
                        typedef typename std::pair<BAT<v2_head_select_t, v2_tail_select_t>*, AN_indicator_vector*> result_t;

                        static result_t filter(BAT<Head, Tail> * arg, tail_t && th1, tail_t && th2, tail_select_t ATR = 1, // for reencoding
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
                            const resoid_t Aoid = std::get<15>(*v2_resoid_t::As);
                            auto result = std::make_pair(ahead::bat::ops::skeletonTail<v2_head_select_t, v2_tail_select_t>(arg), new AN_indicator_vector);
                            result.first->head.metaData = ColumnMetaData(sizeof(head_select_t), AHead, AHeadInv, v2_head_select_t::UNENC_MAX_U, v2_head_select_t::UNENC_MIN);
                            result.first->reserve(arg->size() / 2);
                            if (reencode) {
                                result.first->tail.metaData.AN_A = ATR;
                                result.first->tail.metaData.AN_Ainv = ATInvR;
                            }
                            result.second->reserve(32);
                            tail_select_t Areenc = reencode ? (ATailInv * ATR) : 1;

                            auto mmATInv = v2_mm128<tail_select_t>::set1(ATailInv);
                            auto mmATReenc = v2_mm128<tail_select_t>::set1(Areenc);
                            auto mmDMax = v2_mm128<tail_select_t>::set1(TailUnencMaxU);
                            auto mmThreshold1 = v2_mm128<tail_t>::set1(th1);
                            auto mmThreshold2 = v2_mm128<tail_t>::set1(th2);
                            auto szTail = arg->tail.container->size();
                            auto pT = arg->tail.container->data();
                            auto pTEnd = pT + szTail;
                            auto pmmT = reinterpret_cast<__m128i *>(pT);
                            auto pmmTEnd = reinterpret_cast<__m128i *>(pTEnd);
                            auto pRH = reinterpret_cast<head_select_t*>(result.first->head.container->data());
                            auto pRT = reinterpret_cast<tail_select_t*>(result.first->tail.container->data());
                            // we encode the OIDs here already and increase accordingly by multiples of AHead
                            auto mmOID = v2_mm128<head_select_t>::set_inc(arg->head.metaData.seqbase * AHead, AHead); // fill the vector with increasing values starting at seqbase
                            auto mmInc = v2_mm128<head_select_t>::set1((sizeof(__m128i) / sizeof (typename larger_type<head_select_t, tail_select_t>::type_t)) * AHead);

                            constexpr const size_t headsPerMM128 = sizeof(__m128i) / sizeof (head_select_t);
                            constexpr const size_t tailsPerMM128 = sizeof(__m128i) / sizeof (tail_select_t);

                            size_t pos = 0;
                            for (; pmmT <= (pmmTEnd - 1); ++pmmT) {
                                auto mm = _mm_lddqu_si128(pmmT);
                                if (larger_type<head_select_t, tail_t>::isFirstLarger) {
                                    constexpr const size_t factor = sizeof(head_select_t) / sizeof(tail_t);
                                    constexpr const tail_mask_t maskMask = static_cast<tail_mask_t>((1ull << headsPerMM128) - 1);
                                    v2_mm128_AN<tail_t>::detect(mm, mmATInv, mmDMax, result.second, pos, Aoid); // we only need to check the tail types since the head is virtual anyways
                                    // comparison on encoded values
                                    auto mask = v2_mm128_cmp<tail_t, Op1>::cmp_mask(mm, mmThreshold1) & v2_mm128_cmp<tail_t, Op2>::cmp_mask(mm, mmThreshold2);
                                    if (mask) {
                                        auto maskTmp = mask;
                                        for (size_t i = 0; i < factor; ++i) {
                                            auto actMask = maskTmp & maskMask;
                                            if (actMask) {
                                                v2_mm128<head_select_t>::pack_right2(pRH, mmOID, actMask);
                                            }
                                            mmOID = v2_mm128<head_select_t>::add(mmOID, mmInc);
                                            maskTmp >>= headsPerMM128;
                                        }
                                        pos += tailsPerMM128;
                                        if (reencode) {
                                            mm = v2_mm128<tail_t>::mullo(mm, mmATReenc);
                                        }
                                        v2_mm128<tail_t>::pack_right2(pRT, mm, mask);
                                    } else {
                                        mmOID = v2_mm128<head_select_t>::add(mmOID, v2_mm128<head_select_t>::mullo(mmInc, v2_mm128<head_select_t>::set1(factor)));
                                    }
                                } else {
                                    v2_mm128_AN<tail_t>::detect(mm, mmATInv, mmDMax, result.second, pos, Aoid); // we only need to check the tail types since the head is virtual anyways
                                    // comparison on encoded values
                                    auto mask = v2_mm128_cmp<tail_t, Op1>::cmp_mask(mm, mmThreshold1) & v2_mm128_cmp<tail_t, Op2>::cmp_mask(mm, mmThreshold2);
                                    if (mask) {
                                        // the mask will select only the lower OIDs anyways
                                        v2_mm128<head_select_t>::pack_right2(pRH, mmOID, mask);
                                        pos += tailsPerMM128;
                                        if (reencode) {
                                            mm = v2_mm128<tail_t>::mullo(mm, mmATReenc);
                                        }
                                        v2_mm128<tail_t>::pack_right2(pRT, mm, mask);
                                    }
                                    mmOID = v2_mm128<head_select_t>::add(mmOID, mmInc);
                                }
                            }
                            size_t numSelectedValues = pRT - reinterpret_cast<tail_select_t*>(result.first->tail.container->data());
                            result.first->overwrite_size(numSelectedValues); // "register" the number of values we added
                            auto iter = arg->begin();
                            const size_t numFilteredValues = reinterpret_cast<tail_select_t*>(pmmT) - pT;
                            *iter += (numFilteredValues);
                            Op1<tail_t> op1;
                            Op2<tail_t> op2;
                            for (; iter->hasNext(); ++*iter, ++pos) {
                                auto t = iter->tail();
                                if (static_cast<tail_select_t>(t * ATailInv) > TailUnencMaxU) {
                                    result.second->push_back(pos * Aoid);
                                }
                                if (op1(t, th1) & op2(t, th2)) {
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

                    template<template<typename > class Op1, template<typename > class Op2>
                    struct SelectionAN2<Op1, Op2, v2_void_t, v2_str_t, false> {

                        typedef v2_void_t Head;
                        typedef typename Head::type_t head_t;
                        typedef typename TypeMap<Head>::v2_encoded_t::v2_select_t v2_head_select_t;
                        typedef typename v2_head_select_t::type_t head_select_t;
                        typedef v2_str_t v2_tail_select_t;
                        typedef typename std::pair<BAT<v2_head_select_t, v2_tail_select_t>*, AN_indicator_vector*> result_t;

                        static result_t filter(BAT<Head, v2_str_t> * arg, str_t&& threshold1, str_t&& threshold2, __attribute__((unused)) str_t ATR = nullptr, // cuurently only to match the signature
                                __attribute__((unused)) str_t ATInvR = nullptr // cuurently only to match the signature
                                ) {
                            // TODO for now we assume that selection is only done on base BATs!!! Of course, there could be selections on BATs with encoded heads!
                            static_assert(std::is_base_of<v2_base_t, Head>::value, "Head must be a base type");

                            // always encode head (will usually be a conversion from void -> oid)
                            const head_select_t AHead = std::get<v2_head_select_t::As->size() - 1>(*v2_head_select_t::As);
                            const head_select_t AHeadInv = std::get<v2_head_select_t::Ainvs->size() - 1>(*v2_head_select_t::Ainvs);
                            auto result = std::make_pair(ahead::bat::ops::skeletonTail<v2_head_select_t, v2_str_t>(arg), nullptr);
                            result.first->head.metaData = ColumnMetaData(sizeof(head_select_t), AHead, AHeadInv, v2_head_select_t::UNENC_MAX_U, v2_head_select_t::UNENC_MIN);
                            result.first->reserve(arg->size() / 2);
                            auto iter = arg->begin();
                            Op1<int> op1;
                            Op2<int> op2;
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
            }
        }
    }
}

#endif /* SELECT_AN_SSE_TCC */
