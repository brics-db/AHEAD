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

#ifndef SELECT_SSE_TCC
#define SELECT_SSE_TCC

#include <type_traits>

#include <ColumnStore.h>
#include <column_storage/Bat.h>
#include <column_storage/TempBat.h>
#include <column_operators/SSE.hpp>
#include <column_operators/SSECMP.hpp>
#include <column_operators/Normal/miscellaneous.tcc>

namespace v2 {
    namespace bat {
        namespace ops {
            namespace Private {

                template<template<typename > class Op, typename Head, typename Tail>
                struct Selection1 {
                };

                template<template<typename > class Op, typename Tail>
                struct Selection1<Op, v2_void_t, Tail> {

                    typedef typename Tail::type_t tail_t;
                    typedef typename v2_void_t::v2_select_t v2_head_select_t;
                    typedef typename v2_head_select_t::type_t head_select_t;
                    typedef typename Tail::v2_select_t v2_tail_select_t;
                    typedef typename v2_tail_select_t::type_t tail_select_t;
                    typedef BAT<v2_head_select_t, v2_tail_select_t> result_t;

                    static result_t* filter(BAT<v2_void_t, Tail>* arg, tail_t && th) {
                        auto result = skeleton<v2_head_select_t, v2_tail_select_t>(arg);
                        result->reserve(arg->size());
                        auto mmThreshold = v2_mm128<tail_t>::set1(th);
                        auto szTail = arg->tail.container->size();
                        auto pT = arg->tail.container->data();
                        auto pTEnd = pT + szTail;
                        auto pmmT = reinterpret_cast<__m128i *>(pT);
                        auto pmmTEnd = reinterpret_cast<__m128i *>(pTEnd);
                        auto pRH = reinterpret_cast<head_select_t*>(result->head.container->data());
                        auto pmmRH = reinterpret_cast<__m128i *>(pRH);
                        auto pRT = reinterpret_cast<tail_select_t*>(result->tail.container->data());
                        auto pmmRT = reinterpret_cast<__m128i *>(pRT);
                        auto mmOID = v2_mm128<head_select_t>::set_inc(arg->head.metaData.seqbase); // fill the vector with increasing values starting at seqbase
                        auto mmInc = v2_mm128<head_select_t>::set1(sizeof(__m128i) / sizeof (typename larger_type<head_select_t, tail_select_t>::type_t));
                        for (; pmmT <= (pmmTEnd - 1); ++pmmT) {
                            auto mm = _mm_lddqu_si128(pmmT);
                            auto mask = v2_mm128_cmp<tail_t, Op>::cmp_mask(mm, mmThreshold);
                            if (mask) {
                                size_t nMaskOnes = __builtin_popcountll(mask);

                                if (larger_type<head_select_t, tail_t>::isFirstLarger) {
                                    constexpr const size_t factor = sizeof(head_select_t) / sizeof(tail_t);
                                    constexpr const size_t headsPerMM128 = sizeof(__m128i) / sizeof (head_select_t);
                                    decltype(mask) maskMask = static_cast<decltype(mask)>((1ull << headsPerMM128) - 1);
                                    auto maskTmp = mask;
                                    for (size_t i = 0; i < factor; ++i) {
                                        size_t nToAdd = __builtin_popcountll(maskTmp & maskMask);
                                        if (nToAdd) {
                                            auto mmOIDs = v2_mm128<head_select_t>::pack_right(mmOID, maskTmp & maskMask);
                                            _mm_storeu_si128(pmmRH, mmOIDs);
                                            pmmRH = reinterpret_cast<__m128i *>(reinterpret_cast<head_select_t*>(pmmRH) + nToAdd);
                                        }
                                        mmOID = v2_mm128<head_select_t>::add(mmOID, mmInc);
                                        maskTmp >>= headsPerMM128;
                                    }
                                    _mm_storeu_si128(pmmRT, v2_mm128<tail_t>::pack_right(mm, mask));
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
                                        size_t nToAdd = __builtin_popcountll(maskTmp & maskMask);
                                        if (nToAdd) {
                                            _mm_storeu_si128(pmmRT, v2_mm128<tail_t>::pack_right(mm, maskTmp & maskMask));
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
                        result->overwrite_size(reinterpret_cast<decltype(pRT)>(pmmRT) - pRT); // "register" the number of values we added
                        auto iter = arg->begin();
                        *iter += (reinterpret_cast<decltype(pT)>(pmmT) - pT);
                        Op<tail_t> op;
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

                template<template<typename > class Op>
                struct Selection1<Op, v2_void_t, v2_str_t> {

                    typedef typename v2_void_t::v2_select_t v2_head_select_t;
                    typedef typename v2_str_t::v2_select_t v2_tail_select_t;
                    typedef BAT<v2_head_select_t, v2_tail_select_t> result_t;

                    static result_t* filter(BAT<v2_void_t, v2_str_t>* arg, str_t && threshold) {
                        auto result = skeleton<v2_head_select_t, v2_tail_select_t>(arg);
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
                };

                template<template<typename > class Op1, template<typename > class Op2, typename Tail>
                struct Selection2<Op1, Op2, v2_void_t, Tail> {

                    typedef typename Tail::type_t tail_t;
                    typedef typename v2_void_t::v2_select_t v2_head_select_t;
                    typedef typename v2_head_select_t::type_t head_select_t;
                    typedef typename Tail::v2_select_t v2_tail_select_t;
                    typedef typename v2_tail_select_t::type_t tail_select_t;
                    typedef BAT<v2_head_select_t, v2_tail_select_t> result_t;

                    static result_t* filter(BAT<v2_void_t, Tail>* arg, tail_t && th1, tail_t && th2) {
                        auto result = skeleton<v2_head_select_t, v2_tail_select_t>(arg);
                        result->reserve(arg->size());
                        auto mmThreshold1 = v2_mm128<tail_t>::set1(th1);
                        auto mmThreshold2 = v2_mm128<tail_t>::set1(th2);
                        auto szTail = arg->tail.container->size();
                        auto pT = arg->tail.container->data();
                        auto pTEnd = pT + szTail;
                        auto pmmT = reinterpret_cast<__m128i *>(pT);
                        auto pmmTEnd = reinterpret_cast<__m128i *>(pTEnd);
                        auto pRH = reinterpret_cast<head_select_t*>(result->head.container->data());
                        auto pmmRH = reinterpret_cast<__m128i *>(pRH);
                        auto pRT = reinterpret_cast<tail_select_t*>(result->tail.container->data());
                        auto pmmRT = reinterpret_cast<__m128i *>(pRT);
                        auto mmOID = v2_mm128<head_select_t>::set_inc(arg->head.metaData.seqbase); // fill the vector with increasing values starting at seqbase
                        auto mmInc = v2_mm128<head_select_t>::set1(sizeof(__m128i) / sizeof (typename larger_type<head_select_t, tail_select_t>::type_t));
                        for (; pmmT <= (pmmTEnd - 1); ++pmmT) {
                            auto mm = _mm_lddqu_si128(pmmT);
                            auto mask = v2_mm128_cmp<tail_t, Op1>::cmp_mask(mm, mmThreshold1) & v2_mm128_cmp<tail_t, Op2>::cmp_mask(mm, mmThreshold2);
                            if (mask) {
                                size_t nMaskOnes = __builtin_popcountll(mask);

                                if (larger_type<head_select_t, tail_t>::isFirstLarger) {
                                    constexpr const size_t factor = sizeof(head_select_t) / sizeof(tail_t);
                                    constexpr const size_t headsPerMM128 = sizeof(__m128i) / sizeof (head_select_t);
                                    decltype(mask) maskMask = static_cast<decltype(mask)>((1ull << headsPerMM128) - 1);
                                    auto maskTmp = mask;
                                    for (size_t i = 0; i < factor; ++i) {
                                        size_t nToAdd = __builtin_popcountll(maskTmp & maskMask);
                                        if (nToAdd) {
                                            auto mmOIDs = v2_mm128<head_select_t>::pack_right(mmOID, maskTmp & maskMask);
                                            _mm_storeu_si128(pmmRH, mmOIDs);
                                            pmmRH = reinterpret_cast<__m128i *>(reinterpret_cast<head_select_t*>(pmmRH) + nToAdd);
                                        }
                                        mmOID = v2_mm128<head_select_t>::add(mmOID, mmInc);
                                        maskTmp >>= headsPerMM128;
                                    }
                                    _mm_storeu_si128(pmmRT, v2_mm128<tail_t>::pack_right(mm, mask));
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
                                        size_t nToAdd = __builtin_popcountll(maskTmp & maskMask);
                                        if (nToAdd) {
                                            _mm_storeu_si128(pmmRT, v2_mm128<tail_t>::pack_right(mm, maskTmp & maskMask));
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
                        result->overwrite_size(reinterpret_cast<decltype(pRT)>(pmmRT) - pRT); // "register" the number of values we added
                        auto iter = arg->begin();
                        *iter += (reinterpret_cast<decltype(pT)>(pmmT) - pT);
                        Op1<tail_t> op1;
                        Op2<tail_t> op2;
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

                template<template<typename > class Op1, template<typename > class Op2>
                struct Selection2<Op1, Op2, v2_void_t, v2_str_t> {

                    typedef typename v2_void_t::v2_select_t v2_head_select_t;
                    typedef typename v2_head_select_t::type_t head_select_t;
                    typedef typename v2_str_t::v2_select_t v2_tail_select_t;
                    typedef typename v2_tail_select_t::type_t tail_select_t;
                    typedef BAT<v2_head_select_t, v2_tail_select_t> result_t;

                    static result_t* filter(BAT<v2_void_t, v2_str_t>* arg, tail_select_t && th1, tail_select_t && th2) {
                        auto result = skeleton<v2_head_select_t, v2_tail_select_t>(arg);
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
        }
    }
}

#endif /* SELECT_SSE_TCC */
