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
 * File:   aggregate.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 22. November 2016, 16:26
 */

#ifndef AGGREGATE_AN_TCC
#define AGGREGATE_AN_TCC

#include <type_traits>

#include <ColumnStore.h>
#include <column_storage/Bat.h>
#include <column_storage/TempBat.h>
#include <column_operators/SSE.hpp>
#include <column_operators/SSECMP.hpp>
#include <column_operators/SSEAN.hpp>

namespace v2 {
    namespace bat {
        namespace ops {

            /**
             * Multiplies the tail values of each of the two BATs and sums everything up.
             * @param arg1
             * @param arg2
             * @return A single sum of the pair-wise products of the two BATs
             */
            template<typename Result, typename Head1, typename Tail1, typename Head2, typename Tail2, typename ResEnc = typename TypeMap<Result>::v2_encoded_t, typename T1Enc = typename TypeMap<Tail1>::v2_encoded_t, typename T2Enc = typename TypeMap<Tail2>::v2_encoded_t>
            std::tuple<BAT<v2_void_t, Result>*, std::vector<bool>*, std::vector<bool>*>
            aggregate_mul_sumAN (
                                 BAT<Head1, Tail1>* arg1,
                                 BAT<Head2, Tail2>* arg2,
                                 typename Result::type_t init = typename Result::type_t (0),
                                 typename ResEnc::type_t RA = std::get < ANParametersSelector<ResEnc>::As->size () - 1 > (*ANParametersSelector<ResEnc>::As), // use largest A for encoding by default
                                 typename ResEnc::type_t RAInv = std::get < ANParametersSelector<ResEnc>::Ainvs->size () - 1 > (*ANParametersSelector<ResEnc>::Ainvs)
                                 ) {
                typedef typename Result::type_t result_t;
                typedef typename T1Enc::type_t t1enc_t;
                typedef typename T2Enc::type_t t2enc_t;

                t1enc_t AT1inv = arg1->tail.metaData.AN_Ainv;
                t1enc_t AT1unencMaxU = arg1->tail.metaData.AN_unencMaxU;
                t2enc_t AT2inv = arg2->tail.metaData.AN_Ainv;
                t2enc_t AT2unencMaxU = arg2->tail.metaData.AN_unencMaxU;

                const bool isTail1Encoded = std::is_base_of<v2_anencoded_t, Tail1>::value;
                const bool isTail2Encoded = std::is_base_of<v2_anencoded_t, Tail2>::value;
                const bool isResultEncoded = std::is_base_of<v2_anencoded_t, Result>::value;
                result_t total = init;
                std::vector<bool>* vec1 = (isTail1Encoded ? new std::vector<bool>(arg1->size()) : nullptr);
                std::vector<bool>* vec2 = (isTail2Encoded ? new std::vector<bool>(arg2->size()) : nullptr);
                auto iter1 = arg1->begin();
                auto iter2 = arg2->begin();
                for (size_t i = 0; iter1->hasNext() && iter2->hasNext(); ++*iter1, ++*iter2, ++i) {
                    t1enc_t x1 = iter1->tail() * (isTail1Encoded ? AT1inv : 1);
                    t2enc_t x2 = iter2->tail() * (isTail2Encoded ? AT2inv : 1);
                    if (isTail1Encoded && x1 <= AT1unencMaxU) {
                        (*vec1)[i] = true;
                    }
                    if (isTail2Encoded && x2 <= AT2unencMaxU) {
                        (*vec2)[i] = true;
                    }
                    total += static_cast<result_t>(x1) * static_cast<result_t>(x2);
                }
                if (isResultEncoded)
                    total *= RA;
                delete iter2;
                delete iter1;
                typedef typename TempBAT<v2_void_t, Result>::coldesc_head_t cd_head_t;
                typedef typename TempBAT<v2_void_t, Result>::coldesc_tail_t cd_tail_t;
                auto bat = new TempBAT<v2_void_t, Result>(cd_head_t(), cd_tail_t(ColumnMetaData(sizeof (result_t), RA, RAInv, Result::UNENC_MAX_U, Result::UNENC_MIN)));
                bat->append(total);
                return std::make_tuple(bat, vec1, vec2);
            }

            /**
             * Multiplies the tail values of each of the two BATs and sums everything up.
             * @param arg1
             * @param arg2
             * @return A single sum of the pair-wise products of the two BATs
             */
            template<typename Result, typename Head1, typename Tail1, typename Head2, typename Tail2, typename ResEnc = typename TypeMap<Result>::v2_encoded_t, typename T1Enc = typename TypeMap<Tail1>::v2_encoded_t, typename T2Enc = typename TypeMap<Tail2>::v2_encoded_t>
            std::tuple<BAT<v2_void_t, Result>*, std::vector<bool>*, std::vector<bool>*>
            aggregate_mul_sumAN_SSE (
                                     BAT<Head1, Tail1>* arg1,
                                     BAT<Head2, Tail2>* arg2,
                                     typename Result::type_t init = typename Result::type_t (0),
                                     typename ResEnc::type_t RA = std::get < ANParametersSelector<ResEnc>::As->size () - 1 > (*ANParametersSelector<ResEnc>::As), // use largest A for encoding by default
                                     typename ResEnc::type_t RAInv = std::get < ANParametersSelector<ResEnc>::Ainvs->size () - 1 > (*ANParametersSelector<ResEnc>::Ainvs)
                                     ) {
                typedef typename Tail1::type_t tail1_t;
                typedef typename Tail2::type_t tail2_t;
                typedef typename Result::type_t result_t;
                typedef typename T1Enc::type_t t1enc_t;
                typedef typename T2Enc::type_t t2enc_t;

                t1enc_t AT1inv = static_cast<t1enc_t>(arg1->tail.metaData.AN_Ainv);
                t1enc_t AT1unencMaxU = static_cast<t1enc_t>(arg1->tail.metaData.AN_unencMaxU);
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
                auto pmmT1 = reinterpret_cast<__m128i*>(pT1);
                auto pmmT1End = reinterpret_cast<__m128i*>(pT1End);
                auto pT2 = arg2->tail.container->data();
                auto pT2End = pT2 + szTail2;
                auto pmmT2 = reinterpret_cast<__m128i*>(pT2);
                auto pmmT2End = reinterpret_cast<__m128i*>(pT2End);
                auto mmTotal = _mm_set1_epi64x(0ll);
                auto mmAT1inv = v2_mm128<t1enc_t>::set1(AT1inv);
                auto mmDMax1 = v2_mm128<t1enc_t>::set1(AT1unencMaxU + 1);
                auto mmAT2inv = v2_mm128<t2enc_t>::set1(AT2inv);
                auto mmDMax2 = v2_mm128<t2enc_t>::set1(AT2unencMaxU + 1);
                size_t i = 0;
                for (; pmmT1 <= (pmmT1End - 1) && pmmT2 <= (pmmT2End - 1); i += (sizeof (__m128i) / sizeof (typename v2_smaller_type<t1enc_t, t2enc_t>::type_t))) {
                    __m128i mmDec1, mmDec2;
                    if (v2_larger_type<tail1_t, tail2_t>::isFirstLarger) {
                        constexpr const size_t factor = sizeof (tail1_t) / sizeof (tail2_t);
                        constexpr const size_t steps = sizeof (__m128i) / sizeof (tail1_t);
                        static_assert(factor <= 4, "factor must be of size 4, but is larger");
                        if (isTail2Encoded) {
                            v2_mm128_AN_detect<tail2_t>(mmDec2, *pmmT2++, mmAT2inv, mmDMax2, vec2, i);
                        } else {
                            mmDec2 = *pmmT2++;
                        }
                        if (isTail1Encoded) {
                            v2_mm128_AN_detect<tail1_t>(mmDec1, *pmmT1++, mmAT1inv, mmDMax1, vec1, i);
                        } else {
                            mmDec1 = *pmmT1++;
                        }
                        auto mmTemp = v2_mm128_mullo<tail1_t, 0, tail2_t, 0 * steps, result_t>()(mmDec1, mmDec2);
                        mmTotal = v2_mm128<result_t>::add(mmTotal, mmTemp);
                        if (factor > 1) {
                            if (isTail1Encoded) {
                                v2_mm128_AN_detect<tail1_t>(mmDec1, *pmmT1++, mmAT1inv, mmDMax1, vec1, i);
                            } else {
                                mmDec1 = *pmmT1++;
                            }
                            auto mmTemp = v2_mm128_mullo<tail1_t, 0, tail2_t, 1 * steps, result_t>()(mmDec1, mmDec2);
                            mmTotal = v2_mm128<result_t>::add(mmTotal, mmTemp);
                        } else {
                            std::stringstream ss;
                            ss << '[' << __FILE__ << ':' << __LINE__ << " (" << __func__ << ")] The second tail type is not at least double the size of the first tail type -- as it is expected to be!";
                            throw std::runtime_error(ss.str());
                        }
                        if (factor > 2) {
                            if (isTail1Encoded) {
                                v2_mm128_AN_detect<tail1_t>(mmDec1, *pmmT1++, mmAT1inv, mmDMax1, vec1, i);
                            } else {
                                mmDec1 = *pmmT1++;
                            }
                            auto mmTemp = v2_mm128_mullo<tail1_t, 0, tail2_t, 2 * steps, result_t>()(mmDec1, mmDec2);
                            mmTotal = v2_mm128<result_t>::add(mmTotal, mmTemp);
                        }
                        if (factor > 3) {
                            if (isTail1Encoded) {
                                v2_mm128_AN_detect<tail1_t>(mmDec1, *pmmT1++, mmAT1inv, mmDMax1, vec1, i);
                            } else {
                                mmDec1 = *pmmT1++;
                            }
                            auto mmTemp = v2_mm128_mullo<tail1_t, 0, tail2_t, 3 * steps, result_t>()(mmDec1, mmDec2);
                            mmTotal = v2_mm128<result_t>::add(mmTotal, mmTemp);
                        }
                    } else {
                        // either the second is larger, or both have the same width
                        // the code is the same since for the latter the for-loop runs only once
                        // the compiler should compile the loop away in that case
                        constexpr const size_t factor = sizeof (tail2_t) / sizeof (tail1_t);
                        constexpr const size_t steps = sizeof (__m128i) / sizeof (tail2_t);
                        static_assert(factor <= 4, "factor must be of size 4, but is larger");
                        if (isTail1Encoded) {
                            v2_mm128_AN_detect<tail1_t>(mmDec1, *pmmT1++, mmAT1inv, mmDMax1, vec1, i);
                        } else {
                            mmDec1 = *pmmT1++;
                        }
                        if (isTail2Encoded) {
                            v2_mm128_AN_detect<tail2_t>(mmDec2, *pmmT2++, mmAT2inv, mmDMax2, vec2, i);
                        } else {
                            mmDec2 = *pmmT2++;
                        }
                        auto mmTemp = v2_mm128_mullo<tail1_t, 0 * steps, tail2_t, 0, result_t>()(mmDec1, mmDec2);
                        mmTotal = v2_mm128<result_t>::add(mmTotal, mmTemp);
                        if (factor > 1) {
                            if (isTail2Encoded) {
                                v2_mm128_AN_detect<tail2_t>(mmDec2, *pmmT2++, mmAT2inv, mmDMax2, vec2, i);
                            } else {
                                mmDec2 = *pmmT2++;
                            }
                            auto mmTemp = v2_mm128_mullo<tail1_t, 1 * steps, tail2_t, 0, result_t>()(mmDec1, mmDec2);
                            mmTotal = v2_mm128<result_t>::add(mmTotal, mmTemp);
                        }
                        if (factor > 2) {
                            if (isTail2Encoded) {
                                v2_mm128_AN_detect<tail2_t>(mmDec2, *pmmT2++, mmAT2inv, mmDMax2, vec2, i);
                            } else {
                                mmDec2 = *pmmT2++;
                            }
                            auto mmTemp = v2_mm128_mullo<tail1_t, 2 * steps, tail2_t, 0, result_t>()(mmDec1, mmDec2);
                            mmTotal = v2_mm128<result_t>::add(mmTotal, mmTemp);
                        }
                        if (factor > 3) {
                            if (isTail2Encoded) {
                                v2_mm128_AN_detect<tail2_t>(mmDec2, *pmmT2++, mmAT2inv, mmDMax2, vec2, i);
                            } else {
                                mmDec2 = *pmmT2++;
                            }
                            auto mmTemp = v2_mm128_mullo<tail1_t, 3 * steps, tail2_t, 0, result_t>()(mmDec1, mmDec2);
                            mmTotal = v2_mm128<result_t>::add(mmTotal, mmTemp);
                        }
                    }
                }
                auto total = init + v2_mm128<result_t>::sum(mmTotal);
                pT1 = reinterpret_cast<tail1_t*>(pmmT1);
                pT2 = reinterpret_cast<tail2_t*>(pmmT2);
                for (; pT1 < pT1End && pT2 < pT2End; ++pT1, ++pT2, ++i) {
                    auto t1 = *pT1;
                    if (isTail1Encoded) {
                        t1 *= AT1inv;
                        if (t1 > AT1unencMaxU) {
                            (*vec1)[i] = true;
                        }
                    }
                    auto t2 = *pT2;
                    if (isTail2Encoded) {
                        t2 *= AT2inv;
                        if (t2 > AT2unencMaxU) {
                            (*vec2)[i] = true;
                        }
                    }
                    total += t1 * t2;
                }
                if (isResultEncoded) {
                    total *= RA;
                }
                typedef typename TempBAT<v2_void_t, Result>::coldesc_head_t cd_head_t;
                typedef typename TempBAT<v2_void_t, Result>::coldesc_tail_t cd_tail_t;
                auto bat = new TempBAT<v2_void_t, Result>(cd_head_t(), cd_tail_t(ColumnMetaData(sizeof (result_t), RA, RAInv, Result::UNENC_MAX_U, Result::UNENC_MIN)));
                bat->append(total);
                return std::make_tuple(bat, vec1, vec2);
            }
        }
    }
}

#endif /* AGGREGATE_AN_TCC */
