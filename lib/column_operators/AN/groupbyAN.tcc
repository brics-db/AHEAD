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
 * File:   groupbyAN.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 22. November 2016, 16:27
 */

#ifndef GROUPBY_AN_TCC
#define GROUPBY_AN_TCC

#include <type_traits>

#include <google/dense_hash_map>

#include <column_storage/TempStorage.hpp>
#include <column_operators/ANbase.hpp>
#include "../miscellaneous.hpp"
#include "ANhelper.tcc"

namespace ahead {
    namespace bat {
        namespace ops {

            namespace Private {

                template<typename Head, typename Tail, bool reencode>
                struct groupbyAN {

                    typedef ANhelper<Head> head_helper_t;
                    typedef ANhelper<Tail> tail_helper_t;
                    typedef typename head_helper_t::type_t head_t;
                    typedef typename tail_helper_t::type_t tail_t;
                    typedef typename TypeMap<Tail>::v2_base_t::type_t tunenc_t;
                    typedef ahead::bat::ops::Private::hash<tail_t> hasher;
                    typedef ahead::bat::ops::Private::equals<tail_t> comparator;

                    static std::tuple<BAT<v2_void_t, v2_resoid_t>*, BAT<v2_void_t, v2_resoid_t>*, AN_indicator_vector *, AN_indicator_vector *> unary(
                            BAT<Head, Tail>* bat,
                            resoid_t AOID,
                            resoid_t AOIDinv) {
                        const head_t HAInv = head_helper_t::getIfEncoded(bat->head.metaData.AN_Ainv);
                        const head_t HUnencMaxU = head_helper_t::getIfEncoded(bat->head.metaData.AN_unencMaxU);
                        const tail_t TAInv = tail_helper_t::getIfEncoded(bat->tail.metaData.AN_Ainv);
                        const tail_t TUnencMaxU = tail_helper_t::getIfEncoded(bat->tail.metaData.AN_unencMaxU);
                        AN_indicator_vector *vec1 = head_helper_t::createIndicatorVector();
                        AN_indicator_vector *vec2 = tail_helper_t::createIndicatorVector();

                        google::dense_hash_map<tunenc_t, oid_t, hasher, comparator> dictionary;
                        dictionary.set_empty_key(Tail::dhm_emptykey);
                        auto batVOIDtoRGID = new TempBAT<v2_void_t, v2_resoid_t>();
                        batVOIDtoRGID->tail.metaData = ColumnMetaData(size_bytes<resoid_t>, AOID, AOIDinv, v2_resoid_t::UNENC_MAX_U, v2_resoid_t::UNENC_MIN);
                        auto batVGIDtoROID = new TempBAT<v2_void_t, v2_resoid_t>();
                        batVGIDtoROID->tail.metaData = ColumnMetaData(size_bytes<resoid_t>, AOID, AOIDinv, v2_resoid_t::UNENC_MAX_U, v2_resoid_t::UNENC_MIN);

                        auto iter = bat->begin();
                        for (size_t i = 0; iter->hasNext(); ++*iter, ++i) {
                            head_t h = head_helper_t::mulIfEncoded(iter->head(), HAInv);
                            if (head_helper_t::isEncoded && (h > HUnencMaxU)) {
                                vec1->push_back(i * AOID);
                            }
                            tail_t t1 = tail_helper_t::mulIfEncoded(iter->tail(), TAInv);
                            if (tail_helper_t::isEncoded && (t1 > TUnencMaxU)) {
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

                        const head_t HAInv = head_helper_t::getIfEncoded(bat->head.metaData.AN_Ainv);
                        const head_t HUnencMaxU = head_helper_t::getIfEncoded(bat->head.metaData.AN_unencMaxU);
                        const tail_t TAInv = tail_helper_t::getIfEncoded(bat->tail.metaData.AN_Ainv);
                        const tail_t TUnencMaxU = tail_helper_t::getIfEncoded(bat->tail.metaData.AN_unencMaxU);
                        resoid_t GAInv = static_cast<resoid_t>(grouping->tail.metaData.AN_Ainv);
                        resoid_t GUnencMaxU = static_cast<resoid_t>(grouping->tail.metaData.AN_unencMaxU);
                        AN_indicator_vector *vec1 = head_helper_t::createIndicatorVector();
                        AN_indicator_vector *vec2 = tail_helper_t::createIndicatorVector();
                        AN_indicator_vector * vecGrouping = new AN_indicator_vector;
                        vecGrouping->reserve(32);

                        // auto batHashToRGID = new TempBAT<v2_largerTail_t, v2_resoid_t>();
                        // batHashToRGID->tail.metaData = ColumnMetaData(size_bytes<resoid_t>, AOID, AOIDinv, v2_resoid_t::UNENC_MAX_U, v2_resoid_t::UNENC_MIN);
                        google::dense_hash_map<largerTail_t, oid_t> dictionary;
                        dictionary.set_empty_key(v2_largerTail_t::dhm_emptykey);
                        auto batVOIDtoRGID = new TempBAT<v2_void_t, v2_resoid_t>();
                        batVOIDtoRGID->tail.metaData = ColumnMetaData(size_bytes<resoid_t>, AOID, AOIDinv, v2_resoid_t::UNENC_MAX_U, v2_resoid_t::UNENC_MIN);
                        auto batVGIDtoROID = new TempBAT<v2_void_t, v2_resoid_t>();
                        batVGIDtoROID->tail.metaData = ColumnMetaData(size_bytes<resoid_t>, AOID, AOIDinv, v2_resoid_t::UNENC_MAX_U, v2_resoid_t::UNENC_MIN);

                        auto iter = bat->begin();
                        auto iterG = grouping->begin();
                        for (size_t i = 0; iter->hasNext(); ++*iter, ++*iterG, ++i) {
                            head_t h = head_helper_t::mulIfEncoded(iter->head(), HAInv);
                            if (head_helper_t::isEncoded && (h > HUnencMaxU)) {
                                vec1->push_back(i * AOID);
                            }
                            tail_t t = tail_helper_t::mulIfEncoded(iter->tail(), TAInv);
                            if (tail_helper_t::isEncoded && (t > TUnencMaxU)) {
                                vec2->push_back(i * AOID);
                            }
                            resoid_t g = ANhelper<v2_resoid_t, resoid_t>::mulIfEncoded(iterG->tail(), GAInv);
                            if (g > GUnencMaxU) {
                                vecGrouping->push_back(i * AOID);
                            }
                            largerTail_t curHash = static_cast<largerTail_t>(hasher::get(t)) * static_cast<largerTail_t>(numGroups) + static_cast<largerTail_t>(g);
                            // search this tail in our mapping
                            // idx is the void value of mapGIDtoTail, which starts at zero
                            // auto opt = findFirstHead(batHashToRGID, curHash);
                            auto iterDict = dictionary.find(curHash);
                            // if (opt) {
                            if (iterDict != dictionary.end()) {
                                // batVOIDtoRGID->append(opt.value());
                                batVOIDtoRGID->append(iterDict->second);
                            } else {
                                resoid_t newRGID = batVGIDtoROID->size() * AOID;
                                batVGIDtoROID->append(h * AOID);
                                // batHashToRGID->append(std::make_pair(curHash, newRGID));
                                dictionary[curHash] = newRGID;
                                batVOIDtoRGID->append(newRGID);
                            }
                        }
                        delete iter;
                        delete iterG;
                        // delete batHashToRGID;
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
