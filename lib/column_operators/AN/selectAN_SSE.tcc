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

#include "../miscellaneous.hpp"
#include "SSEAN.hpp"
#include "ANhelper.tcc"
#include <util/utility.hpp>

#ifdef __GNUC__
#pragma GCC target "sse4.2"
#else
#warning "Forcing SSE 4.2 code is not yet implemented for this compiler"
#endif

namespace ahead {
    namespace bat {
        namespace ops {
            namespace simd {
                namespace sse {
                    namespace Private {

                        template<template<typename > class Op, typename Head, typename Tail>
                        struct SelectionAN1;

                        template<template<typename > class Op, typename Tail>
                        struct SelectionAN1<Op, v2_void_t, Tail> {

                            typedef v2_void_t Head;
                            typedef typename Head::type_t head_t;
                            typedef typename Tail::type_t tail_t;
                            typedef typename TypeMap<Head>::v2_encoded_t::v2_select_t v2_head_select_t;
                            typedef typename v2_head_select_t::type_t head_select_t;
                            typedef typename Tail::v2_select_t v2_tail_select_t;
                            typedef typename v2_tail_select_t::type_t tail_select_t;
                            typedef typename mm_op<__m128i, tail_t, Op>::mask_t tail_mask_t;
                            typedef typename std::pair<BAT<v2_head_select_t, v2_tail_select_t>*, AN_indicator_vector*> result_t;
                            typedef ANhelper<Tail> tail_helper_t;

                            static result_t filter(
                                    BAT<Head, Tail>* arg,
                                    tail_t th,
                                    resoid_t AOID) {
                                static_assert(std::is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                                static_assert(tail_helper_t::isEncoded, "Tail must be an AN-encoded type");

                                // always encode head (will usually be a conversion from void -> oid)
                                const head_select_t AHead = std::get<v2_head_select_t::As->size() - 1>(*v2_head_select_t::As);
                                const head_select_t AHeadInv = std::get<v2_head_select_t::Ainvs->size() - 1>(*v2_head_select_t::Ainvs);
                                const tail_select_t ATailInv = static_cast<tail_t>(arg->tail.metaData.AN_Ainv);
                                const tail_select_t TailUnencMaxU = static_cast<tail_t>(arg->tail.metaData.AN_unencMaxU);
                                auto vec = tail_helper_t::createIndicatorVector();
                                auto pIndicatorPos = vec->data();
                                // TODO we would have to reserve as much space as we assume that there will be corrupt values
                                auto result = ahead::bat::ops::skeletonTail<v2_head_select_t, v2_tail_select_t>(arg);
                                result->head.metaData = ColumnMetaData(size_bytes<head_select_t>, AHead, AHeadInv, v2_head_select_t::UNENC_MAX_U, v2_head_select_t::UNENC_MIN);
                                result->reserve_head(arg->size());

                                auto mmATInv = mm<__m128i, tail_select_t>::set1(ATailInv);
                                auto mmDMax = mm<__m128i, tail_select_t>::set1(TailUnencMaxU);
                                auto mmThreshold = mm<__m128i, tail_t>::set1(th);
                                auto szTail = arg->tail.container->size();
                                auto pT = arg->tail.container->data();
                                auto pTEnd = pT + szTail;
                                auto pmmT = reinterpret_cast<__m128i *>(pT);
                                auto pmmTEnd = reinterpret_cast<__m128i *>(pTEnd);
                                auto pRH = reinterpret_cast<head_select_t*>(result->head.container->data());
                                // we encode the OIDs here already and increase accordingly by multiples of AHead
                                resoid_t posEnc = arg->head.metaData.seqbase * AHead;
                                auto mmOID = mm<__m128i, head_select_t>::set_inc(posEnc, AHead); // fill the vector with increasing values starting at seqbase
                                auto mmInc = mm128<head_select_t>::set1((sizeof(__m128i) / sizeof (typename larger_type<head_select_t, tail_select_t>::type_t)) * AHead); // increase OIDs by number of larger types per vector

                                const constexpr size_t headsPerMM128 = sizeof(__m128i) / sizeof (head_select_t);

                                size_t numCorruptedValues = 0;
                                for (; pmmT <= (pmmTEnd - 1); ++pmmT) {
                                    auto mmTmp = _mm_stream_load_si128(pmmT);
                                    auto mask = mm_op<__m128i, tail_t, Op>::cmp_mask(mmTmp, mmThreshold); // comparison on encoded values
                                    if (larger_type<head_select_t, tail_t>::isFirstLarger) {
                                        const constexpr size_t ratioHeadPerTail = sizeof(head_select_t) / sizeof(tail_t);
                                        const constexpr tail_mask_t maskMask = static_cast<tail_mask_t>((1ull << headsPerMM128) - 1);
                                        auto maskTmp = mask;
                                        for (size_t i = 0; i < ratioHeadPerTail; ++i) {
                                            auto actMask = maskTmp & maskMask;
                                            mm<__m128i, head_select_t>::pack_right2(pRH, mmOID, actMask);
                                            mmOID = mm<__m128i, head_select_t>::add(mmOID, mmInc);
                                            maskTmp >>= headsPerMM128;
                                        }
                                    } else {
                                        // the mask will select only the lower OIDs anyways
                                        mm<__m128i, head_select_t>::pack_right2(pRH, mmOID, mask);
                                        mmOID = mm<__m128i, head_select_t>::add(mmOID, mmInc);
                                    }
                                    numCorruptedValues += mmAN<__m128i, tail_t>::detect(mmTmp, mmATInv, mmDMax, pIndicatorPos, posEnc, AOID); // we only need to check the tail types since the head is virtual anyways
                                }

                                overwrite_vector_size(vec, numCorruptedValues);
                                const size_t numSelectedValues = pRH - reinterpret_cast<head_select_t*>(result->head.container->data());
                                result->overwrite_size_head(numSelectedValues); // "register" the number of values we added
                                auto iter = arg->begin();
                                const size_t numFilteredValues = reinterpret_cast<tail_select_t*>(pmmT) - pT;
                                *iter += (numFilteredValues);
                                Op<tail_t> op;
                                for (size_t pos = (reinterpret_cast<decltype(pT)>(pmmT) - pT); iter->hasNext(); ++*iter, ++pos) {
                                    auto t = iter->tail();
                                    if (static_cast<tail_select_t>(t * ATailInv) > TailUnencMaxU) {
                                        vec->push_back(pos * AOID);
                                    }
                                    if (op(t, th)) {
                                        result->append_head(iter->head() * AHead);
                                    }
                                }
                                delete iter;

                                return std::make_pair(result, vec);
                            }
                        };

                        template<template<typename > class Op>
                        struct SelectionAN1<Op, v2_void_t, v2_str_t> {

                            typedef v2_void_t Head;
                            typedef typename Head::type_t head_t;
                            typedef typename TypeMap<Head>::v2_encoded_t::v2_select_t v2_head_select_t;
                            typedef typename v2_head_select_t::type_t head_select_t;
                            typedef typename v2_str_t::v2_select_t v2_tail_select_t;
                            typedef typename std::pair<BAT<v2_head_select_t, v2_tail_select_t>*, AN_indicator_vector*> result_t;

                            static result_t filter(
                                    BAT<Head, v2_str_t> * arg,
                                    str_t threshold,
                                    __attribute__ ((unused)) resoid_t AOID,
                                    __attribute__ ((unused)) str_t ATR = nullptr,
                                    __attribute__ ((unused)) str_t ATInvR = nullptr) {
                                static_assert(std::is_base_of<v2_base_t, Head>::value, "Head must be a base type");

                                // always encode head (will usually be a conversion from void -> oid)
                                const head_select_t AHead = std::get<v2_head_select_t::As->size() - 1>(*v2_head_select_t::As);
                                const head_select_t AHeadInv = std::get<v2_head_select_t::Ainvs->size() - 1>(*v2_head_select_t::Ainvs);
                                auto result = ahead::bat::ops::skeletonTail<v2_head_select_t, v2_str_t>(arg);
                                result->head.metaData = ColumnMetaData(size_bytes<head_select_t>, AHead, AHeadInv, v2_head_select_t::UNENC_MAX_U, v2_head_select_t::UNENC_MIN);
                                result->reserve_head(arg->size());
                                auto iter = arg->begin();
                                Op<int> op;
                                for (; iter->hasNext(); ++*iter) {
                                    auto t = iter->tail();
                                    if (op(strcmp(t, threshold), 0)) {
                                        result->append_head(iter->head() * AHead);
                                    }
                                }
                                delete iter;

                                return std::make_pair(result, nullptr);
                            }
                        };

                        template<template<typename > class Op1, template<typename > class Op2, template<typename > class OpCombine, typename Head, typename Tail>
                        struct SelectionAN2;

                        template<template<typename > class Op1, template<typename > class Op2, template<typename > class OpCombine, typename Tail>
                        struct SelectionAN2<Op1, Op2, OpCombine, v2_void_t, Tail> {

                            typedef v2_void_t Head;
                            typedef typename Head::type_t head_t;
                            typedef typename Tail::type_t tail_t;
                            typedef typename TypeMap<Head>::v2_encoded_t::v2_select_t v2_head_select_t;
                            typedef typename v2_head_select_t::type_t head_select_t;
                            typedef typename Tail::v2_select_t v2_tail_select_t;
                            typedef typename v2_tail_select_t::type_t tail_select_t;
                            typedef typename mm_op<__m128i, tail_t, Op1>::mask_t tail_mask_t;
                            typedef typename std::pair<BAT<v2_head_select_t, v2_tail_select_t>*, AN_indicator_vector*> result_t;
                            typedef ANhelper<Tail> tail_helper_t;

                            static result_t filter(
                                    BAT<Head, Tail> * arg,
                                    tail_t th1,
                                    tail_t th2,
                                    resoid_t AOID) {
                                static_assert(std::is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                                static_assert(tail_helper_t::isEncoded, "Tail must be an AN-encoded type");
                                static_assert(std::is_base_of<ahead::functor, OpCombine<void>>::value, "OpCombine template parameter must be a functor (see include/column_operators/functors.hpp)");

                                // always encode head (will usually be a conversion from void -> oid)
                                const head_select_t AHead = std::get<v2_head_select_t::As->size() - 1>(*v2_head_select_t::As);
                                const head_select_t AHeadInv = std::get<v2_head_select_t::Ainvs->size() - 1>(*v2_head_select_t::Ainvs);
                                const tail_select_t ATailInv = static_cast<tail_select_t>(arg->tail.metaData.AN_Ainv);
                                const tail_select_t TailUnencMaxU = static_cast<tail_select_t>(arg->tail.metaData.AN_unencMaxU);
                                auto vec = tail_helper_t::createIndicatorVector();
                                auto pIndicatorPos = vec->data();
                                // TODO we would have to reserve as much space as we assume that there will be corrupt values
                                auto result = ahead::bat::ops::skeletonTail<v2_head_select_t, v2_tail_select_t>(arg);
                                result->head.metaData = ColumnMetaData(size_bytes<head_select_t>, AHead, AHeadInv, v2_head_select_t::UNENC_MAX_U, v2_head_select_t::UNENC_MIN);
                                result->reserve_head(arg->size());

                                auto mmATInv = mm<__m128i, tail_select_t>::set1(ATailInv);
                                auto mmDMax = mm<__m128i, tail_select_t>::set1(TailUnencMaxU);
                                auto mmThreshold1 = mm<__m128i, tail_t>::set1(th1);
                                auto mmThreshold2 = mm<__m128i, tail_t>::set1(th2);
                                auto szTail = arg->tail.container->size();
                                auto pT = arg->tail.container->data();
                                auto pTEnd = pT + szTail;
                                auto pmmT = reinterpret_cast<__m128i *>(pT);
                                auto pmmTEnd = reinterpret_cast<__m128i *>(pTEnd);
                                auto pRH = reinterpret_cast<head_select_t*>(result->head.container->data());
                                // we encode the OIDs here already and increase accordingly by multiples of AHead
                                resoid_t posEnc = arg->head.metaData.seqbase * AHead;
                                auto mmOID = mm<__m128i, head_select_t>::set_inc(posEnc, AHead); // fill the vector with increasing values starting at seqbase
                                auto mmInc = mm<__m128i, head_select_t>::set1((sizeof(__m128i) / sizeof (typename larger_type<head_select_t, tail_select_t>::type_t)) * AHead);

                                const constexpr size_t headsPerMM128 = sizeof(__m128i) / sizeof (head_select_t);

                                size_t numCorruptedValues = 0;
                                for (; pmmT <= (pmmTEnd - 1); ++pmmT) {
                                    auto mmTmp = _mm_stream_load_si128(pmmT);
                                    // comparison on encoded values
                                    auto res1 = mm_op<__m128i, tail_t, Op1>::cmp(mmTmp, mmThreshold1);
                                    auto res2 = mm_op<__m128i, tail_t, Op2>::cmp(mmTmp, mmThreshold2);
                                    auto mask = mm_op<__m128i, tail_t, OpCombine>::cmp_mask(res1, res2);
                                    if (larger_type<head_select_t, tail_t>::isFirstLarger) {
                                        const constexpr size_t factor = sizeof(head_select_t) / sizeof(tail_t);
                                        const constexpr tail_mask_t maskMask = static_cast<tail_mask_t>((1ull << headsPerMM128) - 1);
                                        auto maskTmp = mask;
                                        for (size_t i = 0; i < factor; ++i) {
                                            auto actMask = maskTmp & maskMask;
                                            mm<__m128i, head_select_t>::pack_right2(pRH, mmOID, actMask);
                                            mmOID = mm<__m128i, head_select_t>::add(mmOID, mmInc);
                                            maskTmp >>= headsPerMM128;
                                        }
                                    } else {
                                        // the mask will select only the lower OIDs anyways
                                        mm<__m128i, head_select_t>::pack_right2(pRH, mmOID, mask);
                                        mmOID = mm<__m128i, head_select_t>::add(mmOID, mmInc);
                                    }
                                    numCorruptedValues += mmAN<__m128i, tail_t>::detect(mmTmp, mmATInv, mmDMax, pIndicatorPos, posEnc, AOID); // we only need to check the tail types since the head is virtual anyways
                                }

                                overwrite_vector_size(vec, numCorruptedValues);
                                size_t numSelectedValues = pRH - reinterpret_cast<head_select_t*>(result->head.container->data());
                                result->overwrite_size_head(numSelectedValues); // "register" the number of values we added
                                auto iter = arg->begin();
                                const size_t numFilteredValues = reinterpret_cast<tail_select_t*>(pmmT) - pT;
                                *iter += (numFilteredValues);
                                Op1<tail_t> op1;
                                Op2<tail_t> op2;
                                for (size_t pos = (reinterpret_cast<decltype(pT)>(pmmT) - pT); iter->hasNext(); ++*iter, ++pos) {
                                    auto t = iter->tail();
                                    if (static_cast<tail_select_t>(t * ATailInv) > TailUnencMaxU) {
                                        vec->push_back(pos * AOID);
                                    }
                                    if (OpCombine<void>()(op1(t, std::forward<tail_t>(th1)), op2(t, std::forward<tail_t>(th2)))) {
                                        result->append_head(iter->head() * AHead);
                                    }
                                }
                                delete iter;

                                return std::make_pair(result, vec);
                            }
                        };

                        template<template<typename > class Op1, template<typename > class Op2, template<typename > class OpCombine>
                        struct SelectionAN2<Op1, Op2, OpCombine, v2_void_t, v2_str_t> {

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
                                    __attribute__ ((unused)) resoid_t AOID,
                                    __attribute__ ((unused)) tail_select_t ATR = nullptr, // currently only to match the signature
                                    __attribute__ ((unused)) tail_select_t ATInvR = nullptr // cururently only to match the signature
                                    ) {
                                static_assert(std::is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                                static_assert(std::is_base_of<ahead::functor, OpCombine<void>>::value, "OpCombine template parameter must be a functor (see include/column_operators/functors.hpp)");

                                // always encode head (will usually be a conversion from void -> oid)
                                const head_select_t AHead = std::get<v2_head_select_t::As->size() - 1>(*v2_head_select_t::As);
                                const head_select_t AHeadInv = std::get<v2_head_select_t::Ainvs->size() - 1>(*v2_head_select_t::Ainvs);
                                auto result = ahead::bat::ops::skeletonTail<v2_head_select_t, v2_str_t>(arg);
                                result->head.metaData = ColumnMetaData(size_bytes<head_select_t>, AHead, AHeadInv, v2_head_select_t::UNENC_MAX_U, v2_head_select_t::UNENC_MIN);
                                result->reserve_head(arg->size());
                                auto iter = arg->begin();
                                Op1<int> op1;
                                Op2<int> op2;
                                for (; iter->hasNext(); ++*iter) {
                                    auto t = iter->tail();
                                    if (OpCombine<void>()(op1(strcmp(t, std::forward<tail_select_t>(threshold1)), 0), op2(strcmp(t, std::forward<tail_select_t>(threshold2)), 0))) {
                                        result->append_head(iter->head() * AHead);
                                    }
                                }
                                delete iter;

                                return std::make_pair(result, nullptr);
                            }
                        };

                    }
                }
            }
        }
    }
}

#endif /* SELECT_AN_SSE_TCC */
