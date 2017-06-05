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

#ifndef AGGREGATE_AN_SEQ_TCC
#define AGGREGATE_AN_SEQ_TCC

#include <type_traits>

#include <column_storage/Storage.hpp>
#include <util/v2typeconversion.hpp>
#include "../miscellaneous.hpp"
#include <column_operators/ANbase.hpp>

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
                std::tuple<BAT<v2_void_t, Result>*, std::vector<bool>*, std::vector<bool>*> aggregate_mul_sumAN(BAT<Head1, Tail1>* arg1, BAT<Head2, Tail2>* arg2, typename Result::type_t init =
                        typename Result::type_t(0), typename ResEnc::type_t RA = std::get<ANParametersSelector<ResEnc>::As->size() - 1>(*ANParametersSelector<ResEnc>::As), // use largest A for encoding by default
                typename ResEnc::type_t RAInv = std::get<ANParametersSelector<ResEnc>::Ainvs->size() - 1>(*ANParametersSelector<ResEnc>::Ainvs)) {
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
                    result_t total = init;
                    std::vector<bool>* vec1 = (isTail1Encoded ? new std::vector<bool>(arg1->size()) : nullptr);
                    std::vector<bool>* vec2 = (isTail2Encoded ? new std::vector<bool>(arg2->size()) : nullptr);

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
#ifdef AN_TEST_ARITH
                    const result_t ATempResultTest = AT1InvR * AT2InvR;
#endif
                    auto iter1 = arg1->begin();
                    auto iter2 = arg2->begin();
                    for (size_t i = 0; iter1->hasNext() && iter2->hasNext(); ++*iter1, ++*iter2, ++i) {
                        auto t1 = iter1->tail();
                        auto t2 = iter2->tail();
                        if (isTail1Encoded && static_cast<t1enc_t>(t1 * AT1inv) > AT1unencMaxU) {
                            (*vec1)[i] = true;
                        }
                        if (isTail2Encoded && static_cast<t2enc_t>(t2 * AT2inv) > AT2unencMaxU) {
                            (*vec2)[i] = true;
                        }
#ifdef AN_TEST_ARITH
                        if (isTail1Encoded || isTail2Encoded) {
                            result_t dTemp = static_cast<result_t>(t1) * static_cast<result_t>(t2);
                            // try 3 times to get a valid result
                            for (size_t i = 0; (static_cast<result_t>(dTemp * ATempResultTest) > static_cast<result_t>(Result::UNENC_MAX_U)) && (i < 3); ++i) {
                                dTemp = static_cast<result_t>(t1) * static_cast<result_t>(t2);
                            }
                            result_t cTemp = dTemp * AResultEncode;
                            result_t totalTemp = total + cTemp;
                            // try 3 times to get a valid result
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

#endif /* AGGREGATE_AN_SEQ_TCC */