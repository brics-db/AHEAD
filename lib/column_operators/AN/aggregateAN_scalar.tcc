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
 * File:   aggregateAN_seq.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 22. November 2016, 16:26
 *
 * File:   aggregateAN_scalar.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Renamed on 28. June 2017, 09:37
 */

#ifndef AGGREGATE_AN_SEQ_TCC
#define AGGREGATE_AN_SEQ_TCC

#include <type_traits>

#include <column_storage/TempStorage.hpp>
#include <util/v2typeconversion.hpp>
#include "../miscellaneous.hpp"
#include <column_operators/ANbase.hpp>
#include "ANhelper.tcc"

#ifdef __GNUC__
#pragma GCC target "no-sse"
#else
#warning "Forcing scalar code is not yet implemented for this compiler"
#endif

namespace ahead {
    namespace bat {
        namespace ops {
            namespace scalar {

                /**
                 * Multiplies the tail values of each of the two BATs and sums everything up.
                 * @param arg1
                 * @param arg2
                 * @return A single sum of the pair-wise products of the two BATs
                 */
                template<typename Result, typename Head1, typename Tail1, typename Head2, typename Tail2, typename ResEnc = typename TypeMap<Result>::v2_encoded_t, typename T1Enc = typename TypeMap<
                        Tail1>::v2_encoded_t, typename T2Enc = typename TypeMap<Tail2>::v2_encoded_t>
                std::tuple<BAT<v2_void_t, Result>*, AN_indicator_vector *, AN_indicator_vector *> aggregate_mul_sumAN(
                        BAT<Head1, Tail1>* arg1,
                        BAT<Head2, Tail2>* arg2,
                        typename Result::type_t init,
                        typename ResEnc::type_t RA,
                        typename ResEnc::type_t RAInv,
                        resoid_t AOID) {
                    typedef typename Result::type_t result_t;
                    typedef typename T1Enc::type_t t1enc_t;
                    typedef typename T2Enc::type_t t2enc_t;
                    typedef ANhelper<Tail1> tail1_helper_t;
                    typedef ANhelper<Tail2> tail2_helper_t;
                    typedef ANhelper<Result> result_helper_t;

                    t1enc_t AT1 = static_cast<t1enc_t>(arg1->tail.metaData.AN_A);
                    t1enc_t AT1inv = static_cast<t1enc_t>(arg1->tail.metaData.AN_Ainv);
                    t1enc_t AT1unencMaxU = static_cast<t1enc_t>(arg1->tail.metaData.AN_unencMaxU);
                    t2enc_t AT2 = static_cast<t2enc_t>(arg2->tail.metaData.AN_A);
                    t2enc_t AT2inv = static_cast<t2enc_t>(arg2->tail.metaData.AN_Ainv);
                    t2enc_t AT2unencMaxU = static_cast<t2enc_t>(arg2->tail.metaData.AN_unencMaxU);

                    result_t total = init;
                    AN_indicator_vector * vec1 = tail1_helper_t::createIndicatorVector();
                    AN_indicator_vector * vec2 = tail2_helper_t::createIndicatorVector();

                    const result_t AResultEncode = (result_helper_t::isEncoded ? RA : result_t(1))
                            * (tail1_helper_t::isEncoded ? (v2convert<result_t>(ext_euclidean(uint128_t(AT1), sizeof(result_t) * 8))) : result_t(1))
                            * (tail2_helper_t::isEncoded ? (v2convert<result_t>(ext_euclidean(uint128_t(AT2), sizeof(result_t) * 8))) : result_t(1));
#ifdef AN_TEST_ARITH
                    const result_t ATempResultTest = AT1InvR * AT2InvR;
#endif
                    auto iter1 = arg1->begin();
                    auto iter2 = arg2->begin();
                    for (size_t i = 0; iter1->hasNext() && iter2->hasNext(); ++*iter1, ++*iter2, ++i) {
                        auto t1 = iter1->tail();
                        auto t2 = iter2->tail();
                        if (tail1_helper_t::isEncoded && static_cast<t1enc_t>(t1 * AT1inv) > AT1unencMaxU) {
                            vec1->push_back(i * AOID);
                        }
                        if (tail2_helper_t::isEncoded && static_cast<t2enc_t>(t2 * AT2inv) > AT2unencMaxU) {
                            vec2->push_back(i * AOID);
                        }
#ifdef AN_TEST_ARITH
                        if (isTail1Encoded || isTail2Encoded) {
                            result_t dTemp = static_cast<result_t>(t1) * static_cast<result_t>(t2);
                            // try at most 3 times to get a valid result
                            for (size_t i = 0; (static_cast<result_t>(dTemp * ATempResultTest) > static_cast<result_t>(Result::UNENC_MAX_U)) && (i < 3); ++i) {
                                dTemp = static_cast<result_t>(t1) * static_cast<result_t>(t2);
                            }
                            result_t cTemp = dTemp * AResultEncode;
                            result_t totalTemp = total + cTemp;
                            // try at most 3 times to get a valid result
                            for (size_t i = 0; (static_cast<result_t>(totalTemp * RAInv) > static_cast<result_t>(Result::UNENC_MAX_U)) && (i < 3); ++i) {
                                totalTemp = total + cTemp;
                            }
                            total = totalTemp;
                        } else {
                            total += static_cast<result_t>(t1) * static_cast<result_t>(t2);
                        }
                    }
#else
                        total += static_cast<result_t>(t1) * static_cast<result_t>(t2);
                    }
                    total *= AResultEncode;
#endif

                    delete iter2;
                    delete iter1;
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

#ifdef __GNUC__
#pragma GCC target "sse4.2"
#else
#warning "Unforcing scalar code is not yet implemented for this compiler"
#endif

#endif /* AGGREGATE_AN_SEQ_TCC */
