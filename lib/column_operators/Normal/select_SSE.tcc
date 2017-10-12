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

#include "../miscellaneous.hpp"
#include "../SIMD/SSE.hpp"

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
                        struct Selection1 {
                        };

                        template<template<typename > class Op, typename Tail>
                        struct Selection1<Op, v2_void_t, Tail> {

                            typedef typename Tail::type_t tail_t;
                            typedef typename v2_void_t::v2_select_t v2_head_select_t;
                            typedef typename v2_head_select_t::type_t head_select_t;
                            typedef typename Tail::v2_select_t v2_tail_select_t;
                            typedef typename v2_tail_select_t::type_t tail_select_t;
                            typedef typename mm_op<__m128i, tail_t, Op>::mask_t tail_mask_t;
                            typedef BAT<v2_head_select_t, v2_tail_select_t> result_t;

                            static result_t* filter(
                                    BAT<v2_void_t, Tail>* arg,
                                    tail_t th) {
                                auto result = skeleton<v2_head_select_t, v2_tail_select_t>(arg);
                                result->reserve_head(arg->size());
                                auto mmThreshold = mm<__m128i, tail_t>::set1(th);
                                auto szTail = arg->tail.container->size();
                                auto pT = arg->tail.container->data();
                                auto pTEnd = pT + szTail;
                                auto pmmT = reinterpret_cast<__m128i *>(pT);
                                auto pmmTEnd = reinterpret_cast<__m128i *>(pTEnd);
                                auto pRH = reinterpret_cast<head_select_t*>(result->head.container->data());
                                auto mmOID = mm<__m128i, head_select_t>::set_inc(arg->head.metaData.seqbase); // fill the vector with increasing values starting at seqbase
                                auto mmInc = mm<__m128i, head_select_t>::set1(sizeof(__m128i) / sizeof (typename larger_type<head_select_t, tail_select_t>::type_t)); // increase OIDs by number of larger types per vector

                                const constexpr size_t headsPerMM128 = sizeof(__m128i) / sizeof (head_select_t);

                                for (; pmmT <= (pmmTEnd - 1); ++pmmT) {
                                    auto mmTmp = _mm_lddqu_si128(pmmT);
                                    auto mask = mm_op<__m128i, tail_t, Op>::cmp_mask(mmTmp, mmThreshold);
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
                                        mm<__m128i, head_select_t>::pack_right2(pRH, mmOID, mask);
                                        mmOID = mm<__m128i, head_select_t>::add(mmOID, mmInc);
                                    }
                                }

                                const size_t numSelectedValues = pRH - reinterpret_cast<head_select_t*>(result->head.container->data());
                                result->overwrite_size(numSelectedValues); // "register" the number of values we added
                                auto iter = arg->begin();
                                const size_t numFilteredValues = reinterpret_cast<tail_select_t*>(pmmT) - pT;
                                *iter += numFilteredValues;
                                Op<tail_t> op;
                                for (; iter->hasNext(); ++*iter) {
                                    auto t = iter->tail();
                                    if (op(t, th)) {
                                        result->append_head(iter->head());
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

                            static result_t* filter(
                                    BAT<v2_void_t, v2_str_t>* arg,
                                    str_t threshold) {
                                auto result = skeleton<v2_head_select_t, v2_tail_select_t>(arg);
                                result->reserve_head(arg->size());
                                auto iter = arg->begin();
                                Op<int> op;
                                for (; iter->hasNext(); ++*iter) {
                                    auto t = iter->tail();
                                    if (op(strcmp(t, threshold), 0)) {
                                        result->append_head(iter->head());
                                    }
                                }
                                delete iter;

                                return result;
                            }
                        };

                        template<template<typename > class Op1, template<typename > class Op2, template<typename > class OpCombine, typename Head, typename Tail>
                        struct Selection2 {
                        };

                        template<template<typename > class Op1, template<typename > class Op2, template<typename > class OpCombine, typename Tail>
                        struct Selection2<Op1, Op2, OpCombine, v2_void_t, Tail> {

                            typedef typename Tail::type_t tail_t;
                            typedef typename v2_void_t::v2_select_t v2_head_select_t;
                            typedef typename v2_head_select_t::type_t head_select_t;
                            typedef typename Tail::v2_select_t v2_tail_select_t;
                            typedef typename v2_tail_select_t::type_t tail_select_t;
                            typedef typename mm_op<__m128i, tail_t, Op1>::mask_t tail_mask_t;
                            typedef BAT<v2_head_select_t, v2_tail_select_t> result_t;

                            static result_t* filter(
                                    BAT<v2_void_t, Tail>* arg,
                                    tail_t th1,
                                    tail_t th2) {
                                static_assert(std::is_base_of<ahead::functor, OpCombine<void>>::value, "OpCombine template parameter must be a functor (see include/column_operators/functors.hpp)");
                                auto result = skeleton<v2_head_select_t, v2_tail_select_t>(arg);
                                result->reserve_head(arg->size());
                                auto mmThreshold1 = mm<__m128i, tail_t>::set1(th1);
                                auto mmThreshold2 = mm<__m128i, tail_t>::set1(th2);
                                auto szTail = arg->tail.container->size();
                                auto pT = arg->tail.container->data();
                                auto pTEnd = pT + szTail;
                                auto pmmT = reinterpret_cast<__m128i *>(pT);
                                auto pmmTEnd = reinterpret_cast<__m128i *>(pTEnd);
                                auto pRH = reinterpret_cast<head_select_t*>(result->head.container->data());
                                auto mmOID = mm<__m128i, head_select_t>::set_inc(arg->head.metaData.seqbase); // fill the vector with increasing values starting at seqbase
                                auto mmInc = mm<__m128i, head_select_t>::set1(sizeof(__m128i) / sizeof (typename larger_type<head_select_t, tail_select_t>::type_t));

                                for (; pmmT <= (pmmTEnd - 1); ++pmmT) {
                                    auto mmTmp = _mm_lddqu_si128(pmmT);
                                    auto res1 = mm_op<__m128i, tail_t, Op1>::cmp(mmTmp, mmThreshold1);
                                    auto res2 = mm_op<__m128i, tail_t, Op2>::cmp(mmTmp, mmThreshold2);
                                    auto mask = mm_op<__m128i, tail_t, OpCombine>::cmp_mask(res1, res2);
                                    if (larger_type<head_select_t, tail_t>::isFirstLarger) {
                                        const constexpr size_t factor = sizeof(head_select_t) / sizeof(tail_t);
                                        const constexpr size_t headsPerMM128 = sizeof(__m128i) / sizeof (head_select_t);
                                        const constexpr tail_mask_t maskMask = static_cast<tail_mask_t>((1ull << headsPerMM128) - 1);
                                        auto maskTmp = mask;
                                        for (size_t i = 0; i < factor; ++i) {
                                            auto actMask = maskTmp & maskMask;
                                            mm<__m128i, head_select_t>::pack_right2(pRH, mmOID, actMask);
                                            mmOID = mm<__m128i, head_select_t>::add(mmOID, mmInc);
                                            maskTmp >>= headsPerMM128;
                                        }
                                    } else {
                                        mm<__m128i, head_select_t>::pack_right2(pRH, mmOID, mask);
                                        mmOID = mm<__m128i, head_select_t>::add(mmOID, mmInc);
                                    }
                                }

                                const size_t numSelectedValues = pRH - reinterpret_cast<head_select_t*>(result->head.container->data());
                                result->overwrite_size_head(numSelectedValues); // "register" the number of values we added
                                auto iter = arg->begin();
                                const size_t numFilteredValues = reinterpret_cast<tail_select_t*>(pmmT) - pT;
                                *iter += numFilteredValues;
                                Op1<tail_t> op1;
                                Op2<tail_t> op2;
                                for (; iter->hasNext(); ++*iter) {
                                    auto t = iter->tail();
                                    if (OpCombine<void>()(op1(t, std::forward<tail_t>(th1)), op2(t, std::forward<tail_t>(th2)))) {
                                        result->append_head(iter->head());
                                    }
                                }
                                delete iter;

                                return result;
                            }
                        };

                        template<template<typename > class Op1, template<typename > class Op2, template<typename > class OpCombine>
                        struct Selection2<Op1, Op2, OpCombine, v2_void_t, v2_str_t> {

                            typedef typename v2_void_t::v2_select_t v2_head_select_t;
                            typedef typename v2_head_select_t::type_t head_select_t;
                            typedef typename v2_str_t::v2_select_t v2_tail_select_t;
                            typedef typename v2_tail_select_t::type_t tail_select_t;
                            typedef BAT<v2_head_select_t, v2_tail_select_t> result_t;

                            static result_t* filter(
                                    BAT<v2_void_t, v2_str_t>* arg,
                                    tail_select_t th1,
                                    tail_select_t th2) {
                                static_assert(std::is_base_of<ahead::functor, OpCombine<void>>::value, "OpCombine template parameter must be a functor (see include/column_operators/functors.hpp)");
                                auto result = skeleton<v2_head_select_t, v2_tail_select_t>(arg);
                                result->reserve_head(arg->size());
                                auto iter = arg->begin();
                                Op1<int> op1;
                                Op2<int> op2;
                                for (; iter->hasNext(); ++*iter) {
                                    auto t = iter->tail();
                                    if (OpCombine<void>()(op1(strcmp(t, std::forward<tail_select_t>(th1)), 0), op2(strcmp(t, std::forward<tail_select_t>(th2)), 0))) {
                                        result->append_head(iter->head());
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
}

#endif /* SELECT_SSE_TCC */
