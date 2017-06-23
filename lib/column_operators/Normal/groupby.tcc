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
 * Created on 15. Dezember 2016, 00:34
 */

#ifndef GROUPBY_TCC
#define GROUPBY_TCC

#include <stdexcept>
#include <type_traits>

#include <google/dense_hash_map>

#include <column_storage/Storage.hpp>
#include "../miscellaneous.hpp"

namespace ahead {
    namespace bat {
        namespace ops {

            namespace Private {

                /**
                 * Group-By the tail
                 * Returns 2 BAT's:
                 * 1) Mapping VOID -> GroupID
                 * 2) GroupID -> OID (first occurrence of Tail for the according group)
                 */
                template<typename Head, typename Tail>
                struct groupby {

                    typedef typename Head::type_t head_t;
                    typedef typename Tail::type_t tail_t;
                    typedef ahead::bat::ops::Private::hash<tail_t> hasher;
                    typedef ahead::bat::ops::Private::equals<tail_t> comparator;

                    /**
                     * Unary group by
                     */
                    static std::pair<BAT<v2_void_t, v2_oid_t> *, BAT<v2_void_t, v2_oid_t> *> unary(
                            BAT<Head, Tail> * bat) {
                        google::dense_hash_map<tail_t, oid_t, hasher, comparator> dictionary;
                        dictionary.set_empty_key(Tail::dhm_emptykey);
                        auto batVoidToGID = new TempBAT<v2_void_t, v2_oid_t>();
                        auto batVGIDtoOID = new TempBAT<v2_void_t, v2_oid_t>();
                        auto iter = bat->begin();
                        for (; iter->hasNext(); ++*iter) {
                            tail_t curTail = iter->tail();
                            // search this tail in our mapping
                            // idx is the void value of mapGIDtoTail, which starts at zero
                            auto iterDict = dictionary.find(curTail);
                            if (iterDict != dictionary.end()) {
                                batVoidToGID->append(iterDict->second);
                            } else {
                                oid_t newGID = batVGIDtoOID->size();
                                batVGIDtoOID->append(iter->head());
                                batVoidToGID->append(newGID);
                                dictionary[curTail] = newGID;
                            }
                        }
                        delete iter;
                        return std::make_pair(batVoidToGID, batVGIDtoOID);
                    }

                    typedef typename Tail::v2_larger_t v2_largerTail_t;
                    typedef typename v2_largerTail_t::type_t largerTail_t;

                    /**
                     * Binary group by
                     */
                    static std::pair<BAT<v2_void_t, v2_oid_t> *, BAT<v2_void_t, v2_oid_t> *> binary(
                            BAT<Head, Tail> * bat,
                            BAT<v2_void_t, v2_oid_t> * grouping,
                            size_t numGroups) {
                        if (bat->size() != grouping->size()) {
                            throw std::runtime_error("input BAT and existing grouping have different sizes!");
                        }
                        // auto batHashToGID = new TempBAT<v2_largerTail_t, v2_oid_t>();
                        google::dense_hash_map<largerTail_t, oid_t> dictionary;
                        dictionary.set_empty_key(v2_largerTail_t::dhm_emptykey);
                        auto batVoidToGID = new TempBAT<v2_void_t, v2_oid_t>();
                        auto batVGIDtoOID = new TempBAT<v2_void_t, v2_oid_t>();
                        auto iter = bat->begin();
                        auto iterG = grouping->begin();
                        for (; iter->hasNext(); ++*iter, ++*iterG) {
                            largerTail_t curHash = static_cast<largerTail_t>(hasher::get(iter->tail())) * static_cast<largerTail_t>(numGroups) + static_cast<largerTail_t>(iterG->tail());
                            // search this tail in our mapping
                            // idx is the void value of mapGIDtoTail, which starts at zero
                            // auto opt = findFirstHead(batHashToGID, curHash);
                            auto iterDict = dictionary.find(curHash);
                            // if (opt) {
                            if (iterDict != dictionary.end()) {
                                // batVoidToGID->append(opt.value());
                                batVoidToGID->append(iterDict->second);
                            } else {
                                oid_t newGID = batVGIDtoOID->size();
                                batVGIDtoOID->append(iter->head());
                                // batHashToGID->append(std::make_pair(curHash, newGID));
                                dictionary[curHash] = newGID;
                                batVoidToGID->append(newGID);
                            }
                        }
                        delete iter;
                        delete iterG;
                        // delete batHashToGID;
                        return std::make_pair(batVoidToGID, batVGIDtoOID);
                    }
                };

            }

            /**
             * The unary Group By operator
             * @param bat The input BAT
             * @return 2 BAT's:
             * 1) Mapping (V)OID -> GroupID
             * 2) GroupID -> Value
             */
            template<typename Head, typename Tail>
            std::pair<BAT<v2_void_t, v2_oid_t> *, BAT<v2_void_t, v2_oid_t> *> groupby(
                    BAT<Head, Tail> * bat) {
                return Private::groupby<Head, Tail>::unary(bat);
            }

            /**
             * The bianry Group By operator
             * @param bat The input BAT
             * @param grouping The previous grouping
             * @return 2 BAT's:
             * 1) Mapping (V)OID -> GroupID
             * 2) GroupID -> Value
             */
            template<typename Head, typename Tail>
            std::pair<BAT<v2_void_t, v2_oid_t> *, BAT<v2_void_t, v2_oid_t> *> groupby(
                    BAT<Head, Tail> * bat,
                    BAT<v2_void_t, v2_oid_t> * grouping,
                    size_t numGroups) {
                return Private::groupby<Head, Tail>::binary(bat, grouping, numGroups);
            }

        }
    }
}

#endif /* GROUPBY_TCC */
