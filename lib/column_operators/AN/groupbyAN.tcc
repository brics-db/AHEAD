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

namespace ahead {
    namespace bat {
        namespace ops {

            namespace Private {

                template<typename T, typename U, bool>
                struct groupbyANconverter0 {
                    constexpr static T getValue(U const & value) {
                        return static_cast<T>(value);
                    }

                    constexpr static T getValue(U const && value) {
                        return static_cast<T>(value);
                    }

                    constexpr static T decode(U const & encoded, U const & AInv) {
                        return static_cast<T>(encoded * AInv);
                    }

                    constexpr static T decode(U const && encoded, U const && AInv) {
                        return static_cast<T>(encoded * AInv);
                    }
                };

                template<typename T, typename U>
                struct groupbyANconverter0<T, U, false> {
                    constexpr static T getValue(__attribute__((unused)) U const & value) {
                        return T(0);
                    }

                    constexpr static T getValue(__attribute__((unused)) U const && value) {
                        return T(0);
                    }

                    constexpr static T decode(U const & unencoded, __attribute__((unused)) U const & AInv) {
                        return static_cast<T>(unencoded);
                    }

                    constexpr static T decode(U const && unencoded, __attribute__((unused)) U const && AInv) {
                        return static_cast<T>(unencoded);
                    }
                };

                template<typename V2T, typename U = typename V2T::type_t>
                struct groupbyANconverter : public groupbyANconverter0<typename V2T::type_t, U, std::is_base_of<v2_anencoded_t, V2T>::value> {
                };

                template<typename Head, typename Tail, bool reencode>
                struct groupbyAN {

                    typedef typename Head::type_t head_t;
                    typedef typename Tail::type_t tail_t;
                    typedef typename TypeMap<Tail>::v2_base_t::type_t tunenc_t;
                    typedef ahead::bat::ops::Private::hash<tail_t> hasher;
                    typedef ahead::bat::ops::Private::equals<tail_t> comparator;

                    static
                    std::tuple<BAT<v2_void_t, v2_resoid_t>*, BAT<v2_void_t, v2_resoid_t>*, std::vector<bool>*, std::vector<bool>*>
                    unary(
                            BAT<Head, Tail>* bat,
                            resoid_t AOID,
                            resoid_t AOIDinv
                            ) {
                        constexpr const bool isHeadEncoded = std::is_base_of<v2_anencoded_t, Head>::value;
                        constexpr const bool isTailEncoded = std::is_base_of<v2_anencoded_t, Tail>::value;
                        head_t HAInv = groupbyANconverter<Head, uint64_t>::getValue(bat->head.metaData.AN_Ainv);
                        head_t HUnencMaxU = groupbyANconverter<Head, uint64_t>::getValue(bat->head.metaData.AN_unencMaxU);
                        tail_t TAInv = groupbyANconverter<Tail, uint64_t>::getValue(bat->tail.metaData.AN_Ainv);
                        tail_t TUnencMaxU = groupbyANconverter<Tail, uint64_t>::getValue(bat->tail.metaData.AN_unencMaxU);
                        std::vector<bool> *vec1 = (isHeadEncoded ? new std::vector<bool>(bat->size()) : nullptr);
                        std::vector<bool> *vec2 = (isTailEncoded ? new std::vector<bool>(bat->size()) : nullptr);

                        google::dense_hash_map<tunenc_t, oid_t, hasher, comparator> dictionary;
                        dictionary.set_empty_key(Tail::dhm_emptykey);
                        auto batVOIDtoRGID = new TempBAT<v2_void_t, v2_resoid_t>();
                        batVOIDtoRGID->tail.metaData = ColumnMetaData(sizeof(resoid_t), AOID, AOIDinv, v2_resoid_t::UNENC_MAX_U, v2_resoid_t::UNENC_MIN);
                        auto batVGIDtoROID = new TempBAT<v2_void_t, v2_resoid_t>();
                        batVGIDtoROID->tail.metaData = ColumnMetaData(sizeof(resoid_t), AOID, AOIDinv, v2_resoid_t::UNENC_MAX_U, v2_resoid_t::UNENC_MIN);
                        auto iter = bat->begin();
                        for (size_t i = 0; iter->hasNext(); ++*iter, ++i) {
                            head_t h = groupbyANconverter<Head>::decode(iter->head(), HAInv);
                            if (isHeadEncoded && (h > HUnencMaxU)) {
                                (*vec1)[i] = true;
                            }
                            tail_t t1 = groupbyANconverter<Tail>::decode(iter->tail(), TAInv);
                            if (isTailEncoded && (t1 > TUnencMaxU)) {
                                (*vec2)[i] = true;
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

                    static
                    std::tuple<BAT<v2_void_t, v2_resoid_t>*, BAT<v2_void_t, v2_resoid_t>*, std::vector<bool>*, std::vector<bool>*>
                    binary(
                            BAT<Head, Tail>* bat,
                            BAT<v2_void_t, v2_resoid_t> * grouping,
                            resoid_t AOID,
                            resoid_t AOIDinv
                            ) {
                        if (bat->size() != grouping->size()) {
                            throw std::runtime_error("input BAT and existing grouping have different sizes!");
                        }

                        constexpr const bool isHeadEncoded = std::is_base_of<v2_anencoded_t, Head>::value;
                        constexpr const bool isTailEncoded = std::is_base_of<v2_anencoded_t, Tail>::value;
                        head_t HAInv = groupbyANconverter<Head, uint64_t>::getValue(bat->head.metaData.AN_Ainv);
                        head_t HUnencMaxU = groupbyANconverter<Head, uint64_t>::getValue(bat->head.metaData.AN_unencMaxU);
                        tail_t TAInv = groupbyANconverter<Tail, uint64_t>::getValue(bat->tail.metaData.AN_Ainv);
                        tail_t TUnencMaxU = groupbyANconverter<Tail, uint64_t>::getValue(bat->tail.metaData.AN_unencMaxU);
                        std::vector<bool> *vec1 = (isHeadEncoded ? new std::vector<bool>(bat->size()) : nullptr);
                        std::vector<bool> *vec2 = (isTailEncoded ? new std::vector<bool>(bat->size()) : nullptr);

                        const size_t numExistingGroups = grouping->size();
                        auto batHashToRGID = new TempBAT<v2_largerTail_t, v2_resoid_t>();
                        batHashToRGID->tail.metaData = ColumnMetaData(sizeof(resoid_t), AOID, AOIDinv, v2_resoid_t::UNENC_MAX_U, v2_resoid_t::UNENC_MIN);
                        auto batVOIDtoRGID = new TempBAT<v2_void_t, v2_resoid_t>();
                        batVOIDtoRGID->tail.metaData = ColumnMetaData(sizeof(resoid_t), AOID, AOIDinv, v2_resoid_t::UNENC_MAX_U, v2_resoid_t::UNENC_MIN);
                        auto batVGIDtoROID = new TempBAT<v2_void_t, v2_resoid_t>();
                        batVGIDtoROID->tail.metaData = ColumnMetaData(sizeof(resoid_t), AOID, AOIDinv, v2_resoid_t::UNENC_MAX_U, v2_resoid_t::UNENC_MIN);
                        auto iter = bat->begin();
                        auto iter2 = grouping->begin();
                        for (size_t i = 0; iter->hasNext(); ++*iter, ++*iter2, ++i) {
                            head_t h = groupbyANconverter<Head>::decode(iter->head(), HAInv);
                            if (isHeadEncoded && (h > HUnencMaxU)) {
                                (*vec1)[i] = true;
                            }
                            tail_t t = groupbyANconverter<Tail>::decode(iter->tail(), TAInv);
                            if (isTailEncoded && (t > TUnencMaxU)) {
                                (*vec2)[i] = true;
                            }
                            largerTail_t curHash = static_cast<largerTail_t>(hasher::get(t)) * static_cast<largerTail_t>(numExistingGroups) + static_cast<largerTail_t>(h);
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
                        return std::make_tuple(batVOIDtoRGID, batVGIDtoROID, vec1, vec2);
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
            std::tuple<BAT<v2_void_t, v2_resoid_t>*, BAT<v2_void_t, v2_resoid_t>*, std::vector<bool>*, std::vector<bool>*>
            groupbyAN(
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
            std::tuple<BAT<v2_void_t, v2_resoid_t>*, BAT<v2_void_t, v2_resoid_t>*, std::vector<bool>*, std::vector<bool>*>
            groupbyAN(
                    BAT<Head, Tail> * bat,
                    BAT<v2_void_t, v2_resoid_t> * grouping,
                    typename v2_resoid_t::type_t AOID = std::get<ANParametersSelector<v2_resoid_t>::As->size() - 1>(*ANParametersSelector<v2_resoid_t>::As), // use largest A for encoding by default
                    typename v2_resoid_t::type_t AOIDinv = std::get<ANParametersSelector<v2_resoid_t>::Ainvs->size() - 1>(*ANParametersSelector<v2_resoid_t>::Ainvs) // and the appropriate inverse
                    ) {
                return Private::groupbyAN<Head, Tail, false>::binary(bat, grouping, AOID, AOIDinv);
            }

        }
    }
}

#endif /* GROUPBY_AN_TCC */
