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

#include "aggregateAN_SSE.tcc"
#include "aggregateAN_seq.tcc"

namespace ahead {
    namespace bat {
        namespace ops {

            namespace Private {

                template<typename Result, typename Head, typename Tail, typename HEnc = typename TypeMap<Head>::v2_encoded_t, typename TEnc = typename TypeMap<Tail>::v2_encoded_t>
                struct aggregate_sum_groupedAN {
                    typedef typename Result::type_t result_t;
                    typedef typename HEnc::type_t henc_t;
                    typedef typename TEnc::type_t tenc_t;

                    static
                    std::tuple<BAT<v2_void_t, Result> *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *>
                    run(
                            BAT<Head, Tail> * bat,
                            BAT<v2_void_t, v2_resoid_t> * grouping,
                            size_t numGroups,
                            typename Result::type_t AResult,
                            typename Result::type_t AResultInv,
                            resoid_t AOID
                            ) {
                        constexpr const bool isHeadEncoded = std::is_base_of<v2_anencoded_t, Head>::value;
                        constexpr const bool isTailEncoded = std::is_base_of<v2_anencoded_t, Head>::value;

                        if (bat->size() != grouping->size()) {
                            throw std::runtime_error("bat and grouping must have the same size!");
                        }

                        const henc_t AHinv = static_cast<henc_t>(bat->head.metaData.AN_Ainv);
                        const henc_t AHunencMaxU = static_cast<henc_t>(bat->head.metaData.AN_unencMaxU);
                        const tenc_t AT = static_cast<tenc_t>(bat->tail.metaData.AN_A);
                        const tenc_t ATinv = static_cast<tenc_t>(bat->tail.metaData.AN_Ainv);
                        const tenc_t ATunencMaxU = static_cast<tenc_t>(bat->tail.metaData.AN_unencMaxU);
                        const resoid_t AGinv = static_cast<resoid_t>(grouping->tail.metaData.AN_Ainv);
                        const resoid_t AGunencMaxU = static_cast<resoid_t>(grouping->tail.metaData.AN_unencMaxU);

                        typedef typename TempBAT<v2_void_t, Result>::coldesc_head_t cd_head_t;
                        typedef typename TempBAT<v2_void_t, Result>::coldesc_tail_t cd_tail_t;
                        auto batResult = new TempBAT<v2_void_t, Result>(cd_head_t(), cd_tail_t(ColumnMetaData(sizeof(result_t), AResult, AResultInv, Result::UNENC_MAX_U, Result::UNENC_MIN)));
                        AN_indicator_vector * vecHead = isHeadEncoded ? new AN_indicator_vector() : nullptr;
                        if (isHeadEncoded) {
                            vecHead->reserve(32);
                        }
                        AN_indicator_vector * vecTail = isTailEncoded ? new AN_indicator_vector() : nullptr;
                        if (isTailEncoded) {
                            vecTail->reserve(32);
                        }
                        AN_indicator_vector * vecGrouping = new AN_indicator_vector();
                        vecGrouping->reserve(32);

                        const result_t AResultEncode = AResult * (isTailEncoded ? (v2convert<result_t>(ext_euclidean(uint128_t(AT), sizeof(result_t) * 8))) : result_t(1));
#ifdef AN_TEST_ARITH
                        const result_t ATempResultTest = (isTailEncoded ? (v2convert<result_t>(ext_euclidean(uint128_t(AT), sizeof(result_t) * 8))) : result_t(1));
#endif

                        auto vec = batResult->tail.container.get();
                        vec->resize(numGroups, result_t(0));
                        auto iter = bat->begin();
                        auto iterGroup = grouping->begin();
                        for (size_t i = 0; iter->hasNext(); ++*iter, ++*iterGroup, ++i) {
                            if (isHeadEncoded && static_cast<henc_t>(iter->head() * AHinv) > AHunencMaxU) {
                                vecHead->push_back(i * AOID);
                            }
                            auto t = iter->tail();
                            if (isTailEncoded && static_cast<tenc_t>(t * ATinv) > ATunencMaxU) {
                                vecTail->push_back(i * AOID);
                            }
                            auto gID = iterGroup->tail() * AGinv;
                            if (gID > AGunencMaxU) {
                                vecGrouping->push_back(i * AOID);
                            } else {
#ifdef AN_TEST_ARITH
                                if (isHeadEncoded || isTailEncoded) {
                                    const result_t base = (*vec)[iterGroup->tail()];
                                    result_t temp = base + t;
                                    // try at most 3 times to get a valid result
                                    for (size_t i = 0; (i < 3) && (static_cast<result_t>(temp * ATempResultTest)) > static_cast<result_t>(Result::UNENC_MAX_U); ++i) {
                                        temp = base + t;
                                    }
                                    (*vec)[gID] = temp;
                                } else {
                                    (*vec)[gID] += t * AResultEncode;
                                }
#else
                                (*vec)[gID] += t * AResultEncode;
#endif
                            }
                        }
#ifdef AN_TEST_ARITH
                        for (auto & res : *vec) {
                            res *= AResultEncode;
                        }
#endif
                        return std::make_tuple(batResult, vecHead, vecTail, vecGrouping);
                    }
                };

            }

            template<typename Result, typename Head, typename Tail>
            std::tuple<BAT<v2_void_t, Result> *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *>
            aggregate_sum_groupedAN(
                    BAT<Head, Tail> * bat,
                    BAT<v2_void_t, v2_resoid_t> * grouping,
                    size_t numGroups,
                    typename Result::type_t AResult,
                    typename Result::type_t AResultInv,
                    resoid_t AOID
                    ) {
                static_assert(std::is_base_of<v2_anencoded_t, Result>::value, "Result type must be a subtype of v2_anencoded_t!");
                return Private::aggregate_sum_groupedAN<Result, Head, Tail>::run(bat, grouping, numGroups, AResult, AResultInv, AOID);
            }

        }
    }
}

#endif /* AGGREGATE_AN_TCC */
