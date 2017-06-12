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
 * File:   groupby.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 22. November 2016, 16:27
 */

#ifndef GROUPBY_AN_TCC
#define GROUPBY_AN_TCC

#ifdef DEBUG
#include <iostream>
#include <iomanip>
#endif
#include <type_traits>

#include <google/dense_hash_map>

#include <column_storage/Storage.hpp>
#include <column_operators/ANbase.hpp>
#include "../miscellaneous.hpp"
#include "ANhelper.tcc"

namespace ahead {
    namespace bat {
        namespace ops {

            namespace Private {

                template<typename Head, typename Tail, bool reencode>
                struct groupbyAN {

                    typedef typename Head::type_t head_t;
                    typedef typename Tail::type_t tail_t;
                    typedef typename TypeMap<Tail>::v2_base_t::type_t tunenc_t;
                    typedef ahead::bat::ops::Private::hash<tail_t> hasher;
                    typedef ahead::bat::ops::Private::equals<tail_t> comparator;

                    static std::tuple<BAT<v2_void_t, v2_resoid_t>*, BAT<v2_void_t, v2_resoid_t>*, AN_indicator_vector *, AN_indicator_vector *> unary(
                            BAT<Head, Tail>* bat,
                            resoid_t AOID,
                            resoid_t AOIDinv) {
                        constexpr const bool isHeadEncoded = std::is_base_of<v2_anencoded_t, Head>::value;
                        constexpr const bool isTailEncoded = std::is_base_of<v2_anencoded_t, Tail>::value;
                        head_t HAInv = ANhelper<Head, decltype(bat->head.metaData.AN_Ainv)>::getValue(bat->head.metaData.AN_Ainv);
                        head_t HUnencMaxU = ANhelper<Head, decltype(bat->head.metaData.AN_unencMaxU)>::getValue(bat->head.metaData.AN_unencMaxU);
                        tail_t TAInv = ANhelper<Tail, decltype(bat->tail.metaData.AN_Ainv)>::getValue(bat->tail.metaData.AN_Ainv);
                        tail_t TUnencMaxU = ANhelper<Tail, decltype(bat->tail.metaData.AN_unencMaxU)>::getValue(bat->tail.metaData.AN_unencMaxU);
                        AN_indicator_vector *vec1 = (isHeadEncoded ? new AN_indicator_vector : nullptr);
                        if (isHeadEncoded) {
                            vec1->reserve(32);
                        }
                        AN_indicator_vector *vec2 = (isTailEncoded ? new AN_indicator_vector : nullptr);
                        if (isTailEncoded) {
                            vec2->reserve(32);
                        }

                        google::dense_hash_map<tunenc_t, oid_t, hasher, comparator> dictionary;
                        dictionary.set_empty_key(Tail::dhm_emptykey);
                        auto batVOIDtoRGID = new TempBAT<v2_void_t, v2_resoid_t>();
                        batVOIDtoRGID->tail.metaData = ColumnMetaData(sizeof(resoid_t), AOID, AOIDinv, v2_resoid_t::UNENC_MAX_U, v2_resoid_t::UNENC_MIN);
                        auto batVGIDtoROID = new TempBAT<v2_void_t, v2_resoid_t>();
                        batVGIDtoROID->tail.metaData = ColumnMetaData(sizeof(resoid_t), AOID, AOIDinv, v2_resoid_t::UNENC_MAX_U, v2_resoid_t::UNENC_MIN);

                        auto iter = bat->begin();
                        for (size_t i = 0; iter->hasNext(); ++*iter, ++i) {
                            head_t h = ANhelper<Head, head_t>::decode(iter->head(), HAInv);
                            if (isHeadEncoded && (h > HUnencMaxU)) {
                                vec1->push_back(i * AOID);
                            }
                            tail_t t1 = ANhelper<Tail, tail_t>::decode(iter->tail(), TAInv);
                            if (isTailEncoded && (t1 > TUnencMaxU)) {
                                vec2->push_back(i * AOID);
                            }
                            auto t2 = static_cast<tunenc_t>(t1); // use (potentially smaller) unencoded size
                            // search this tail in our mapping
                            // idx is the void value of mapGIDtoTail, which starts at zero
                            auto iterDict = dictionary.find(t2);
                            if (iterDict != dictionary.end()) {
                                batVOIDtoRGID->append(iterDict->second * AOID);
                            } else {
                                resoid_t newGID = batVGIDtoROID->size();
                                batVGIDtoROID->append(iter->head() * AOID);
                                batVOIDtoRGID->append(newGID * AOID);
                                dictionary[t2] = newGID;
                            }
                        }
                        delete iter;
                        return std::make_tuple(batVOIDtoRGID, batVGIDtoROID, vec1, vec2);
                    }

                    typedef typename Tail::v2_larger_t v2_largerTail_t;
                    typedef typename v2_largerTail_t::type_t largerTail_t;

                    static std::tuple<BAT<v2_void_t, v2_resoid_t> *, BAT<v2_void_t, v2_resoid_t> *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *> binary(
                            BAT<Head, Tail>* bat,
                            BAT<v2_void_t, v2_resoid_t> * grouping,
                            size_t numGroups,
                            resoid_t AOID,
                            resoid_t AOIDinv) {
                        if (bat->size() != grouping->size()) {
                            throw std::runtime_error("input BAT and existing grouping have different sizes!");
                        }

                        constexpr const bool isHeadEncoded = std::is_base_of<v2_anencoded_t, Head>::value;
                        constexpr const bool isTailEncoded = std::is_base_of<v2_anencoded_t, Tail>::value;
                        head_t HAInv = ANhelper<Head, decltype(bat->head.metaData.AN_Ainv)>::getValue(bat->head.metaData.AN_Ainv);
                        head_t HUnencMaxU = ANhelper<Head, decltype(bat->head.metaData.AN_unencMaxU)>::getValue(bat->head.metaData.AN_unencMaxU);
                        tail_t TAInv = ANhelper<Tail, decltype(bat->tail.metaData.AN_Ainv)>::getValue(bat->tail.metaData.AN_Ainv);
                        tail_t TUnencMaxU = ANhelper<Tail, decltype(bat->tail.metaData.AN_unencMaxU)>::getValue(bat->tail.metaData.AN_unencMaxU);
                        resoid_t GAInv = ANhelper<v2_resoid_t, decltype(grouping->tail.metaData.AN_Ainv)>::getValue(grouping->tail.metaData.AN_Ainv);
                        resoid_t GUnencMaxU = ANhelper<v2_resoid_t, decltype(grouping->tail.metaData.AN_unencMaxU)>::getValue(grouping->tail.metaData.AN_unencMaxU);
                        AN_indicator_vector * vec1 = (isHeadEncoded ? new AN_indicator_vector : nullptr);
                        if (isHeadEncoded) {
                            vec1->reserve(32);
                        }
                        AN_indicator_vector * vec2 = (isTailEncoded ? new AN_indicator_vector : nullptr);
                        if (isTailEncoded) {
                            vec2->reserve(32);
                        }
                        AN_indicator_vector * vecGrouping = new AN_indicator_vector;
                        vecGrouping->reserve(32);

                        auto batHashToRGID = new TempBAT<v2_largerTail_t, v2_resoid_t>();
                        batHashToRGID->tail.metaData = ColumnMetaData(sizeof(resoid_t), AOID, AOIDinv, v2_resoid_t::UNENC_MAX_U, v2_resoid_t::UNENC_MIN);
                        auto batVOIDtoRGID = new TempBAT<v2_void_t, v2_resoid_t>();
                        batVOIDtoRGID->tail.metaData = ColumnMetaData(sizeof(resoid_t), AOID, AOIDinv, v2_resoid_t::UNENC_MAX_U, v2_resoid_t::UNENC_MIN);
                        auto batVGIDtoROID = new TempBAT<v2_void_t, v2_resoid_t>();
                        batVGIDtoROID->tail.metaData = ColumnMetaData(sizeof(resoid_t), AOID, AOIDinv, v2_resoid_t::UNENC_MAX_U, v2_resoid_t::UNENC_MIN);

                        auto iter = bat->begin();
                        auto iterG = grouping->begin();
                        for (size_t i = 0; iter->hasNext(); ++*iter, ++*iterG, ++i) {
                            head_t h = ANhelper<Head, head_t>::decode(iter->head(), HAInv);
                            if (isHeadEncoded && (h > HUnencMaxU)) {
                                vec1->push_back(i * AOID);
                            }
                            tail_t t = ANhelper<Tail, tail_t>::decode(iter->tail(), TAInv);
                            if (isTailEncoded && (t > TUnencMaxU)) {
                                vec2->push_back(i * AOID);
                            }
                            resoid_t g = ANhelper<v2_resoid_t, resoid_t>::decode(iterG->tail(), GAInv);
                            if (g > GUnencMaxU) {
                                vecGrouping->push_back(i * AOID);
                            }
                            largerTail_t curHash = static_cast<largerTail_t>(hasher::get(t)) * static_cast<largerTail_t>(numGroups) + static_cast<largerTail_t>(g);
                            // search this tail in our mapping
                            // idx is the void value of mapGIDtoTail, which starts at zero
                            auto opt = findFirstHead(batHashToRGID, curHash);
                            if (opt) {
                                batVOIDtoRGID->append(opt.value());
                            } else {
                                resoid_t newRGID = batVGIDtoROID->size() * AOID;
                                batVGIDtoROID->append(h * AOID);
                                batHashToRGID->append(std::make_pair(curHash, newRGID));
                                batVOIDtoRGID->append(newRGID);
                            }
                        }
                        delete iter;
                        delete iterG;
                        delete batHashToRGID;
                        return std::make_tuple(batVOIDtoRGID, batVGIDtoROID, vec1, vec2, vecGrouping);
                    }
                };

            }

            /**
             * The unary Group By operator
             * @param bat The input BAT
             * @param AOID the A to encode the new OIDs
             * @param AOIDinv the inverse of A
             * @return 4-tuple:
             * 1) BAT Mapping: VOID -> GroupID (encoded)
             * 2) BAT Mapping: GroupID -> OID (encoded, first occurrence of Tail for the according group)
             * 3) Bitmap Head: position -> error detected (true) or not (false)
             * 4) Bitmap Tail: position -> error detected (true) or not (false)
             */
            template<typename Head, typename Tail>
            std::tuple<BAT<v2_void_t, v2_resoid_t> *, BAT<v2_void_t, v2_resoid_t> *, AN_indicator_vector *, AN_indicator_vector *> groupbyAN(
                    BAT<Head, Tail> * bat,
                    typename v2_resoid_t::type_t AOID = std::get<ANParametersSelector<v2_resoid_t>::As->size() - 1>(*ANParametersSelector<v2_resoid_t>::As), // use largest A for encoding by default
                    typename v2_resoid_t::type_t AOIDinv = std::get<ANParametersSelector<v2_resoid_t>::Ainvs->size() - 1>(*ANParametersSelector<v2_resoid_t>::Ainvs) // and the appropriate inverse
                            ) {
                return Private::groupbyAN<Head, Tail, false>::unary(bat, AOID, AOIDinv);
            }

            /**
             * The binay Group By operator
             * @param bat The input BAT
             * @param grouping The previous grouping
             * @param AOID the A to encode the new OIDs
             * @param AOIDinv the inverse of A
             * @return 4-tuple:
             * 1) BAT Mapping: VOID -> GroupID (encoded)
             * 2) BAT Mapping: GroupID -> OID (encoded, first occurrence of Tail for the according group)
             * 3) Bitmap Head: position -> error detected (true) or not (false)
             * 4) Bitmap Tail: position -> error detected (true) or not (false)
             */
            template<typename Head, typename Tail>
            std::tuple<BAT<v2_void_t, v2_resoid_t> *, BAT<v2_void_t, v2_resoid_t> *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *> groupbyAN(
                    BAT<Head, Tail> * bat,
                    BAT<v2_void_t, v2_resoid_t> * grouping,
                    size_t numGroups,
                    typename v2_resoid_t::type_t AOID = std::get<ANParametersSelector<v2_resoid_t>::As->size() - 1>(*ANParametersSelector<v2_resoid_t>::As), // use largest A for encoding by default
                    typename v2_resoid_t::type_t AOIDinv = std::get<ANParametersSelector<v2_resoid_t>::Ainvs->size() - 1>(*ANParametersSelector<v2_resoid_t>::Ainvs) // and the appropriate inverse
                            ) {
                return Private::groupbyAN<Head, Tail, false>::binary(bat, grouping, numGroups, AOID, AOIDinv);
            }

        }
    }
}

#endif /* GROUPBY_AN_TCC */
