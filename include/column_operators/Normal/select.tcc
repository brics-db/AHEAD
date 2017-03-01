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

#include <type_traits>

#include <ColumnStore.h>
#include <column_operators/SSE.hpp>
#include <column_operators/SSECMP.hpp>
#include <column_storage/Bat.h>
#include <column_storage/TempBat.h>

namespace v2 {
    namespace bat {
        namespace ops {

            namespace Private {

                template<template<typename > class Op, typename Head, typename Tail>
                struct Selection1 {

                    typedef typename Tail::type_t tail_t;
                    typedef typename Head::v2_select_t head_select_t;
                    typedef typename Tail::v2_select_t tail_select_t;
                    typedef BAT<head_select_t, tail_select_t> bat_t;

                    static bat_t*
                    seq(BAT<Head, Tail>* arg, tail_t && th) {
                        auto result = skeleton<head_select_t, tail_select_t>(arg);
                        result->reserve(arg->size());
                        auto iter = arg->begin();
                        Op<typename Tail::v2_compare_t::type_t> op;
                        for (; iter->hasNext(); ++*iter) {
                            auto t = iter->tail();
                            if (op(t, th)) {
                                result->append(std::make_pair(iter->head(), t));
                            }
                        }
                        delete iter;
                        return result;
                    }
                };

                template<template<typename > class Op, typename Head, typename Tail>
                struct Selection1_SSE {

                    typedef typename Tail::type_t tail_t;
                    typedef typename Head::v2_select_t v2_head_select_t;
                    typedef typename v2_head_select_t::type_t head_select_t;
                    typedef typename Tail::v2_select_t v2_tail_select_t;
                    typedef typename v2_tail_select_t::type_t tail_select_t;
                    typedef BAT<v2_head_select_t, v2_tail_select_t> bat_t;

                    static bat_t*
                    sse42(BAT<v2_void_t, Tail>* arg, tail_t && th) {
                        auto result = skeleton<v2_head_select_t, v2_tail_select_t>(arg);
                        result->reserve(arg->size());
                        auto mmThreshold = v2_mm128<tail_t>::set1(th);
                        oid_t szTail = arg->tail.container->size();
                        auto pT = arg->tail.container->data();
                        auto pTEnd = pT + szTail;
                        auto pmmT = reinterpret_cast<__m128i *>(pT);
                        auto pmmTEnd = reinterpret_cast<__m128i *>(pTEnd);
                        auto pRH = reinterpret_cast<head_select_t*>(result->head.container->data());
                        auto pmmRH = reinterpret_cast<__m128i *>(pRH);
                        auto pRT = reinterpret_cast<tail_select_t*>(result->tail.container->data());
                        auto pmmRT = reinterpret_cast<__m128i *>(pRT);
                        auto mmOID = v2_mm128<head_select_t>::set_inc(arg->head.metaData.seqbase); // fill the vector with increasing values starting at seqbase
                        auto mmInc = v2_mm128<head_select_t>::set1(sizeof(__m128i) / sizeof (head_select_t));
                        size_t valuesAdded = 0, valuesInspected = 0;
                        for (; pmmT <= (pmmTEnd - 1); ++pmmT) {
                            auto mm = _mm_lddqu_si128(pmmT);
                            auto mask = v2_mm128_cmp<tail_t, Op>::cmp_mask(mm, mmThreshold);
                            valuesInspected += larger_type<head_select_t, tail_t>::isFirstLarger ? (sizeof(__m128i) / sizeof (tail_select_t)) : (sizeof (__m128i) / sizeof (head_select_t));
                            if (mask) {
                                size_t nMaskOnes = __builtin_popcount(mask);
                                valuesAdded += nMaskOnes;

                                if (larger_type<head_select_t, tail_t>::isFirstLarger) {
                                    constexpr const size_t factor = sizeof(head_select_t) / sizeof(tail_t);
                                    constexpr const size_t headsPerMM128 = sizeof(__m128i) / sizeof (head_select_t);
                                    decltype(mask) maskMask = static_cast<decltype(mask)>((1ull << headsPerMM128) - 1);
                                    auto maskTmp = mask;
                                    for (size_t i = 0; i < factor; ++i) {
                                        size_t nToAdd = __builtin_popcount(maskTmp & maskMask);
                                        if (nToAdd) {
                                            auto mmOIDs = v2_mm128<head_select_t>::pack_right(mmOID, maskTmp & maskMask);
                                            _mm_storeu_si128(pmmRH, mmOIDs);
                                            pmmRH = reinterpret_cast<__m128i *>(reinterpret_cast<head_select_t*>(pmmRH) + nToAdd);
                                        }
                                        mmOID = v2_mm128<head_select_t>::add(mmOID, mmInc);
                                        maskTmp >>= headsPerMM128;
                                    }
                                    auto mmValues = v2_mm128<tail_t>::pack_right(mm, mask);
                                    _mm_storeu_si128(pmmRT, mmValues);
                                    pmmRT = reinterpret_cast<__m128i *>(reinterpret_cast<tail_select_t*>(pmmRT) + nMaskOnes);
                                } else {
                                    constexpr const size_t factor = sizeof(tail_select_t) / sizeof(head_select_t);
                                    constexpr const size_t tailsPerMM128 = sizeof(__m128i) / sizeof (tail_select_t);
                                    decltype(mask) maskMask = static_cast<decltype(mask)>((1ull << tailsPerMM128) - 1);
                                    auto mmOIDs = v2_mm128<head_select_t>::pack_right(mmOID, mask);
                                    _mm_storeu_si128(pmmRH, mmOIDs);
                                    pmmRH = reinterpret_cast<__m128i *>(reinterpret_cast<head_select_t*>(pmmRH) + nMaskOnes);
                                    mmOID = v2_mm128<head_select_t>::add(mmOID, mmInc);
                                    auto maskTmp = mask;
                                    for (size_t i = 0; i < factor; ++i) {
                                        size_t nToAdd = __builtin_popcount(maskTmp & maskMask);
                                        if (nToAdd) {
                                            auto mmValues = v2_mm128<tail_t>::pack_right(mm, maskTmp & maskMask);
                                            _mm_storeu_si128(pmmRT, mmValues);
                                            pmmRT = reinterpret_cast<__m128i *>(reinterpret_cast<tail_select_t*>(pmmRT) + nToAdd);
                                        }
                                        maskTmp >>= tailsPerMM128;
                                    }
                                }
                            } else {
                                if (larger_type<head_select_t, tail_t>::isFirstLarger) {
                                    constexpr const size_t factor = sizeof(head_select_t) / sizeof(tail_t);
                                    for (size_t i = 0; i < factor; ++i) {
                                        mmOID = v2_mm128<head_select_t>::add(mmOID, mmInc);
                                    }
                                } else {
                                    mmOID = v2_mm128<head_select_t>::add(mmOID, mmInc);
                                }
                            }
                        }
                        result->overwrite_size(valuesAdded); // "register" the number of values we added
                        auto iter = arg->begin();
                        *iter += valuesInspected;
                        Op<tail_t> op;
                        for (; iter->hasNext(); ++*iter) {
                            ++valuesInspected;
                            auto t = iter->tail();
                            if (op(t, th)) {
                                ++valuesAdded;
                                result->append(std::make_pair(iter->head(), t));
                            }
                        }
                        delete iter;
                        return result;
                    }
                };

                template<template<typename > class Op, typename Head>
                struct Selection1<Op, Head, v2_str_t> {

                    typedef typename Head::v2_select_t head_select_t;
                    typedef v2_str_t tail_select_t;
                    typedef BAT<head_select_t, tail_select_t> bat_t;

                    static bat_t*
                    seq(BAT<Head, v2_str_t>* arg, str_t && threshold) {
                        auto result = skeleton<head_select_t, tail_select_t>(arg);
                        result->reserve(arg->size());
                        auto iter = arg->begin();
                        Op<int> op;
                        for (; iter->hasNext(); ++*iter) {
                            auto t = iter->tail();
                            if (op(strcmp(t, threshold), 0)) {
                                result->append(std::make_pair(iter->head(), t));
                            }
                        }
                        return result;
                    }
                };

                template<template<typename > class Op1, template<typename > class Op2, typename Head, typename Tail>
                struct Selection2 {

                    typedef typename Tail::type_t tail_t;
                    typedef typename Head::v2_select_t head_select_t;
                    typedef typename Tail::v2_select_t tail_select_t;
                    typedef BAT<head_select_t, tail_select_t> bat_t;

                    static bat_t*
                    seq(BAT<Head, Tail>* arg, tail_t && th1, tail_t && th2) {
                        auto result = skeleton<head_select_t, tail_select_t>(arg);
                        result->reserve(arg->size());
                        auto iter = arg->begin();
                        Op1<typename Tail::v2_compare_t::type_t> op1;
                        Op2<typename Tail::v2_compare_t::type_t> op2;
                        for (; iter->hasNext(); ++*iter) {
                            auto t = iter->tail();
                            if (op1(t, th1) & op2(t, th2)) {
                                result->append(std::make_pair(iter->head(), t));
                            }
                        }
                        delete iter;
                        return result;
                    }
                };

                template<template<typename > class Op1, template<typename > class Op2, typename Head, typename Tail>
                struct Selection2_SSE {

                    typedef typename Tail::type_t tail_t;
                    typedef typename Head::v2_select_t v2_head_select_t;
                    typedef typename v2_head_select_t::type_t head_select_t;
                    typedef typename Tail::v2_select_t v2_tail_select_t;
                    typedef typename v2_tail_select_t::type_t tail_select_t;
                    typedef BAT<v2_head_select_t, v2_tail_select_t> bat_t;

                    static bat_t*
                    sse42(BAT<v2_void_t, Tail>* arg, tail_t && th1, tail_t && th2) {
                        auto result = skeleton<v2_head_select_t, v2_tail_select_t>(arg);
                        result->reserve(arg->size());
                        auto mmThreshold1 = v2_mm128<tail_t>::set1(th1);
                        auto mmThreshold2 = v2_mm128<tail_t>::set1(th2);
                        oid_t szTail = arg->tail.container->size();
                        auto pT = arg->tail.container->data();
                        auto pTEnd = pT + szTail;
                        auto pmmT = reinterpret_cast<__m128i *>(pT);
                        auto pmmTEnd = reinterpret_cast<__m128i *>(pTEnd);
                        auto pRH = reinterpret_cast<head_select_t*>(result->head.container->data());
                        auto pmmRH = reinterpret_cast<__m128i *>(pRH);
                        auto pRT = reinterpret_cast<tail_select_t*>(result->tail.container->data());
                        auto pmmRT = reinterpret_cast<__m128i *>(pRT);
                        auto mmOID = v2_mm128<head_select_t>::set_inc(arg->head.metaData.seqbase); // fill the vector with increasing values starting at seqbase
                        auto mmInc = v2_mm128<head_select_t>::set1(sizeof(__m128i) / sizeof (head_select_t));
                        size_t valuesAdded = 0, valuesInspected = 0;
                        for (; pmmT <= (pmmTEnd - 1); ++pmmT) {
                            auto mm = _mm_lddqu_si128(pmmT);
                            auto mask = v2_mm128_cmp<tail_t, Op1>::cmp_mask(mm, mmThreshold1) & v2_mm128_cmp<tail_t, Op2>::cmp_mask(mm, mmThreshold2);
                            valuesInspected += larger_type<head_select_t, tail_t>::isFirstLarger ? (sizeof(__m128i) / sizeof (tail_select_t)) : (sizeof (__m128i) / sizeof (head_select_t));
                            if (mask) {
                                size_t nMaskOnes = __builtin_popcount(mask);
                                valuesAdded += nMaskOnes;

                                if (larger_type<head_select_t, tail_t>::isFirstLarger) {
                                    constexpr const size_t factor = sizeof(head_select_t) / sizeof(tail_t);
                                    constexpr const size_t headsPerMM128 = sizeof(__m128i) / sizeof (head_select_t);
                                    decltype(mask) maskMask = static_cast<decltype(mask)>((1ull << headsPerMM128) - 1);
                                    auto maskTmp = mask;
                                    for (size_t i = 0; i < factor; ++i) {
                                        size_t nToAdd = __builtin_popcount(maskTmp & maskMask);
                                        if (nToAdd) {
                                            auto mmOIDs = v2_mm128<head_select_t>::pack_right(mmOID, maskTmp & maskMask);
                                            _mm_storeu_si128(pmmRH, mmOIDs);
                                            pmmRH = reinterpret_cast<__m128i *>(reinterpret_cast<head_select_t*>(pmmRH) + nToAdd);
                                        }
                                        mmOID = v2_mm128<head_select_t>::add(mmOID, mmInc);
                                        maskTmp >>= headsPerMM128;
                                    }
                                    auto mmValues = v2_mm128<tail_t>::pack_right(mm, mask);
                                    _mm_storeu_si128(pmmRT, mmValues);
                                    pmmRT = reinterpret_cast<__m128i *>(reinterpret_cast<tail_select_t*>(pmmRT) + nMaskOnes);
                                } else {
                                    constexpr const size_t factor = sizeof(tail_select_t) / sizeof(head_select_t);
                                    constexpr const size_t tailsPerMM128 = sizeof(__m128i) / sizeof (tail_select_t);
                                    decltype(mask) maskMask = static_cast<decltype(mask)>((1ull << tailsPerMM128) - 1);
                                    auto mmOIDs = v2_mm128<head_select_t>::pack_right(mmOID, mask);
                                    _mm_storeu_si128(pmmRH, mmOIDs);
                                    pmmRH = reinterpret_cast<__m128i *>(reinterpret_cast<head_select_t*>(pmmRH) + nMaskOnes);
                                    mmOID = v2_mm128<head_select_t>::add(mmOID, mmInc);
                                    auto maskTmp = mask;
                                    for (size_t i = 0; i < factor; ++i) {
                                        size_t nToAdd = __builtin_popcount(maskTmp & maskMask);
                                        if (nToAdd) {
                                            auto mmValues = v2_mm128<tail_t>::pack_right(mm, maskTmp & maskMask);
                                            _mm_storeu_si128(pmmRT, mmValues);
                                            pmmRT = reinterpret_cast<__m128i *>(reinterpret_cast<tail_select_t*>(pmmRT) + nToAdd);
                                        }
                                        maskTmp >>= tailsPerMM128;
                                    }
                                }
                            } else {
                                if (larger_type<head_select_t, tail_t>::isFirstLarger) {
                                    constexpr const size_t factor = sizeof(head_select_t) / sizeof(tail_t);
                                    for (size_t i = 0; i < factor; ++i) {
                                        mmOID = v2_mm128<head_select_t>::add(mmOID, mmInc);
                                    }
                                } else {
                                    mmOID = v2_mm128<head_select_t>::add(mmOID, mmInc);
                                }
                            }
                        }
                        result->overwrite_size(valuesAdded); // "register" the number of values we added
                        auto iter = arg->begin();
                        *iter += valuesInspected;
                        Op1<typename Tail::v2_compare_t::type_t> op1;
                        Op2<typename Tail::v2_compare_t::type_t> op2;
                        for (; iter->hasNext(); ++*iter) {
                            ++valuesInspected;
                            auto t = iter->tail();
                            if (op1(t, th1) & op2(t, th2)) {
                                ++valuesAdded;
                                result->append(std::make_pair(iter->head(), t));
                            }
                        }
                        delete iter;
                        return result;
                    }
                };

                template<template<typename > class Op1, template<typename > class Op2, typename Head>
                struct Selection2<Op1, Op2, Head, v2_str_t> {

                    typedef typename Head::v2_select_t head_select_t;
                    typedef v2_str_t tail_select_t;
                    typedef BAT<head_select_t, tail_select_t> bat_t;

                    static bat_t*
                    seq(BAT<Head, v2_str_t>* arg, tail_select_t && th1, tail_select_t && th2) {
                        auto result = skeleton<head_select_t, tail_select_t>(arg);
                        result->reserve(arg->size());
                        auto iter = arg->begin();
                        Op1<int> op1;
                        Op2<int> op2;
                        for (; iter->hasNext(); ++*iter) {
                            auto t = iter->tail();
                            if (op1(strcmp(t, th1), 0) && op2(strcmp(t, th2), 0)) {
                                result->append(std::make_pair(iter->head(), t));
                            }
                        }
                        delete iter;
                        return result;
                    }
                };
            }

            template<template<typename > class Op, typename Head, typename Tail>
            BAT<typename Head::v2_select_t, typename Tail::v2_select_t>*
            select(BAT<Head, Tail>* arg, typename Tail::type_t && th1) {
#ifdef FORCE_SSE
                if (std::is_base_of<v2_str_t, Tail>::value) {
                    return Private::Selection1<Op, Head, Tail>::seq(arg, std::forward<typename Tail::type_t>(th1));
                } else {
                    return Private::Selection1_SSE<Op, v2_void_t, Tail>::sse42(arg, std::forward<typename Tail::type_t>(th1));
                }
#else
                return Private::Selection1<Op, Head, Tail>::seq(arg, std::forward<typename Tail::type_t>(th1));
#endif
            }

            template<template<typename > class Op1 = std::greater_equal, template<typename > class Op2 = std::less_equal, typename Head, typename Tail>
            BAT<typename Head::v2_select_t, typename Tail::v2_select_t>*
            select(BAT<Head, Tail>* arg, typename Tail::type_t && th1, typename Tail::type_t && th2) {
#ifdef FORCE_SSE
                if (std::is_base_of<v2_str_t, Tail>::value) {
                    return Private::Selection2_SSE<Op1, Op2, v2_void_t, Tail>::sse42(arg, std::forward<typename Tail::type_t>(th1), std::forward<typename Tail::type_t>(th2));
                } else {
                    return Private::Selection2<Op1, Op2, Head, Tail>::seq(arg, std::forward<typename Tail::type_t>(th1), std::forward<typename Tail::type_t>(th2));
                }
#else
                return Private::Selection2<Op1, Op2, Head, Tail>::seq(arg, std::forward<typename Tail::type_t>(th1), std::forward<typename Tail::type_t>(th2));
#endif
            }
        }
    }
}

#endif /* SELECT_TCC */
