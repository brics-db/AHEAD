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
 * File:   aggregateAN_SSE.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 22. November 2016, 16:26
 */

#ifndef AGGREGATE_AN_SSE_TCC
#define AGGREGATE_AN_SSE_TCC

#include <type_traits>

#include <column_storage/TempStorage.hpp>
#include <util/v2typeconversion.hpp>
#include <column_operators/ANbase.hpp>
#include "SSEAN.hpp"
#include "../miscellaneous.hpp"

#ifdef __GNUC__
#pragma GCC target "sse4.2"
#else
#warning "Forcing SSE 4.2 code is not yet implemented for this compiler"
#endif

namespace ahead {
    namespace bat {
        namespace ops {
            namespace sse {

                namespace Private {

                    template<typename tail1_t, typename tail2_t, typename result_t, bool isTail1Encoded, bool isTail2Encoded, size_t increment, size_t maxFactor, size_t factor>
                    struct partial_aggregate_mul_sumAN {
                        static inline void doIt(
                                __m128i & mmTotal,
                                __m128i * & pmmT1,
                                __m128i & mmAT1inv,
                                __m128i & mmDMax1,
                                std::vector<bool> * vec1,
                                __m128i * & pmmT2,
                                __m128i & mmAT2inv,
                                __m128i & mmDMax2,
                                std::vector<bool> * vec2,
                                size_t & i) {
                            if (isTail1Encoded) {
                                v2_mm128_AN<tail1_t>::detect(*pmmT1, mmAT1inv, mmDMax1, vec1, i);
                            }
                            if (isTail2Encoded) {
                                v2_mm128_AN<tail2_t>::detect(*pmmT2, mmAT2inv, mmDMax2, vec2, i);
                            }
                            __m128i mmTemp;
                            if (larger_type<tail1_t, tail2_t>::isFirstLarger) {
                                mmTemp = v2_mm128_mullo<tail1_t, 0, tail2_t, (maxFactor - factor) * increment, result_t>(*pmmT1++, *pmmT2);
                            } else if (larger_type<tail1_t, tail2_t>::isSecondLarger) {
                                mmTemp = v2_mm128_mullo<tail1_t, (maxFactor - factor) * increment, tail2_t, 0, result_t>(*pmmT1, *pmmT2++);
                            } else {
                                mmTemp = v2_mm128_mullo<tail1_t, 0, tail2_t, 0, result_t>(*pmmT1, *pmmT2); // we need not increment, as there is essentially no recursion and the advancing is done below
                            }
                            mmTotal = v2_mm128<result_t>::add(mmTotal, mmTemp);
                            i += increment;
                            // if both types have the same size, the following yields in the call below, so there is no recursion
                            partial_aggregate_mul_sumAN<tail1_t, tail2_t, result_t, isTail1Encoded, isTail2Encoded, increment, maxFactor, factor - 1>::doIt(mmTotal, pmmT1, mmAT1inv, mmDMax1, vec1,
                                    pmmT2, mmAT2inv, mmDMax2, vec2, i);
                        }
                    };

                    template<typename tail1_t, typename tail2_t, typename result_t, bool isTail1Encoded, bool isTail2Encoded, size_t increment, size_t maxFactor>
                    struct partial_aggregate_mul_sumAN<tail1_t, tail2_t, result_t, isTail1Encoded, isTail2Encoded, increment, maxFactor, 0> {
                        static inline void doIt(
                                __m128i & mmTotal,
                                __m128i * & pmmT1,
                                __m128i & mmAT1inv,
                                __m128i & mmDMax1,
                                std::vector<bool> * vec1,
                                __m128i * & pmmT2,
                                __m128i & mmAT2inv,
                                __m128i & mmDMax2,
                                std::vector<bool> * vec2,
                                size_t & i) {
                            (void) mmTotal;
                            (void) pmmT1;
                            (void) mmAT1inv;
                            (void) mmDMax1;
                            (void) vec1;
                            (void) pmmT2;
                            (void) mmAT2inv;
                            (void) mmDMax2;
                            (void) vec2;
                            (void) i;
                            if (larger_type<tail1_t, tail2_t>::isFirstLarger) {
                                pmmT2++;
                            } else if (larger_type<tail1_t, tail2_t>::isSecondLarger) {
                                pmmT1++;
                            } else {
                                pmmT1++;
                                pmmT2++;
                            }
                        }
                    };

                }

                /**
                 * Multiplies the tail values of each of the two BATs and sums everything up.
                 * @param arg1
                 * @param arg2
                 * @return A single sum of the pair-wise products of the two BATs
                 */
                template<typename Result, typename Head1, typename Tail1, typename Head2, typename Tail2, typename ResEnc = typename TypeMap<Result>::v2_encoded_t, typename T1Enc = typename TypeMap<
                        Tail1>::v2_encoded_t, typename T2Enc = typename TypeMap<Tail2>::v2_encoded_t>
                std::tuple<BAT<v2_void_t, Result>*, std::vector<bool>*, std::vector<bool>*> aggregate_mul_sumAN(
                        BAT<Head1, Tail1>* arg1,
                        BAT<Head2, Tail2>* arg2,
                        typename Result::type_t init = typename Result::type_t(0),
                        typename ResEnc::type_t RA = std::get<ANParametersSelector<ResEnc>::As->size() - 8>(*ANParametersSelector<ResEnc>::As), // use largest A for encoding by default
                        typename ResEnc::type_t RAInv = std::get<ANParametersSelector<ResEnc>::Ainvs->size() - 8>(*ANParametersSelector<ResEnc>::Ainvs)) {
                    typedef typename Tail1::type_t tail1_t;
                    typedef typename Tail2::type_t tail2_t;
                    typedef typename Result::type_t result_t;
                    typedef typename T1Enc::type_t t1enc_t;
                    typedef typename T2Enc::type_t t2enc_t;

                    t1enc_t AT1 = static_cast<t1enc_t>(arg1->tail.metaData.AN_A);
                    t1enc_t AT1inv = static_cast<t1enc_t>(arg1->tail.metaData.AN_Ainv);
                    t1enc_t AT1unencMaxU = static_cast<t1enc_t>(arg1->tail.metaData.AN_unencMaxU);
                    t2enc_t AT2 = static_cast<t2enc_t>(arg2->tail.metaData.AN_A);
                    t2enc_t AT2inv = static_cast<t2enc_t>(arg2->tail.metaData.AN_Ainv);
                    t2enc_t AT2unencMaxU = static_cast<t2enc_t>(arg2->tail.metaData.AN_unencMaxU);

                    const bool isTail1Encoded = std::is_base_of<v2_anencoded_t, Tail1>::value;
                    const bool isTail2Encoded = std::is_base_of<v2_anencoded_t, Tail2>::value;
                    const bool isResultEncoded = std::is_base_of<v2_anencoded_t, Result>::value;
                    std::vector<bool>* vec1 = (isTail1Encoded ? new std::vector<bool>(arg1->size()) : nullptr);
                    std::vector<bool>* vec2 = (isTail2Encoded ? new std::vector<bool>(arg2->size()) : nullptr);
                    oid_t szTail1 = arg1->tail.container->size();
                    oid_t szTail2 = arg2->tail.container->size();
                    auto pT1 = arg1->tail.container->data();
                    auto pT1End = pT1 + szTail1;
                    auto pmmT1 = reinterpret_cast<__m128i *>(pT1);
                    auto pmmT1End = reinterpret_cast<__m128i *>(pT1End);
                    auto pT2 = arg2->tail.container->data();
                    auto pT2End = pT2 + szTail2;
                    auto pmmT2 = reinterpret_cast<__m128i *>(pT2);
                    auto pmmT2End = reinterpret_cast<__m128i *>(pT2End);
                    auto mmTotal = v2_mm128<result_t>::set1(0);
                    auto mmAT1inv = v2_mm128<t1enc_t>::set1(AT1inv);
                    auto mmDMax1 = v2_mm128<t1enc_t>::set1(AT1unencMaxU);
                    auto mmAT2inv = v2_mm128<t2enc_t>::set1(AT2inv);
                    auto mmDMax2 = v2_mm128<t2enc_t>::set1(AT2unencMaxU);
                    size_t codeWidth = (sizeof(result_t) * 8);
                    uint128_t AT1_ui128(AT1);
                    uint128_t AT2_ui128(AT2);
                    result_t AT1InvR(1);
                    result_t AT2InvR(1);
                    if (isTail1Encoded) {
                        uint128_t temp = ext_euclidean(AT1_ui128, codeWidth);
                        AT1InvR = v2convert<result_t>(temp);
                    }
                    if (isTail2Encoded) {
                        uint128_t temp = ext_euclidean(AT2_ui128, codeWidth);
                        AT2InvR = v2convert<result_t>(temp);
                    }
                    const result_t AResultEncode = AT1InvR * AT2InvR * (isResultEncoded ? RA : result_t(1)); // a single factor for converting the total at the end
                    size_t i = 0;
                    for (; pmmT1 <= (pmmT1End - 1) && pmmT2 <= (pmmT2End - 1);) {
                        if (larger_type<tail1_t, tail2_t>::isFirstLarger) {
                            constexpr const size_t factor = sizeof(tail1_t) / sizeof(tail2_t);
                            constexpr const size_t increment = sizeof(__m128i) / sizeof (tail1_t);
                            Private::partial_aggregate_mul_sumAN<tail1_t, tail2_t, result_t, isTail1Encoded, isTail2Encoded, increment, factor, factor>::doIt(mmTotal, pmmT1, mmAT1inv, mmDMax1, vec1,
                                    pmmT2, mmAT2inv, mmDMax2, vec2, i);
                        } else {
                            // either the second is larger, or both have the same width
                            // the code is the same since for the latter the for-loop runs only once
                            // the compiler should compile the loop away in that case
                            constexpr const size_t factor = sizeof(tail2_t) / sizeof(tail1_t);
                            constexpr const size_t increment = sizeof(__m128i) / sizeof (tail2_t);
                            Private::partial_aggregate_mul_sumAN<tail1_t, tail2_t, result_t, isTail1Encoded, isTail2Encoded, increment, factor, factor>::doIt(mmTotal, pmmT1, mmAT1inv, mmDMax1, vec1,
                                    pmmT2, mmAT2inv, mmDMax2, vec2, i);
                        }
                    }
                    auto total = init + v2_mm128<result_t>::sum(mmTotal);
                    pT1 = reinterpret_cast<tail1_t*>(pmmT1);
                    pT2 = reinterpret_cast<tail2_t*>(pmmT2);
                    for (; pT1 < pT1End && pT2 < pT2End; ++pT1, ++pT2, ++i) {
                        auto t1 = *pT1;
                        auto t2 = *pT2;
                        if (isTail1Encoded && static_cast<t1enc_t>(t1 * AT1inv) > AT1unencMaxU) {
                            (*vec1)[i] = true;
                        }
                        if (isTail2Encoded && static_cast<t2enc_t>(t2 * AT2inv) > AT2unencMaxU) {
                            (*vec2)[i] = true;
                        }
                        total += static_cast<result_t>(t1) * static_cast<result_t>(t2);
                    }
                    total *= AResultEncode;
                    typedef typename TempBAT<v2_void_t, Result>::coldesc_head_t cd_head_t;
                    typedef typename TempBAT<v2_void_t, Result>::coldesc_tail_t cd_tail_t;
                    auto bat = new TempBAT<v2_void_t, Result>(cd_head_t(), cd_tail_t(ColumnMetaData(sizeof(result_t), RA, RAInv, Result::UNENC_MAX_U, Result::UNENC_MIN)));
                    bat->append(total);
                    return std::make_tuple(bat, vec1, vec2);
                }

            }
        }
    }
}

#endif /* AGGREGATE_AN_SSE_TCC */
