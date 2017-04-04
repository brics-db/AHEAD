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

#include <column_storage/Storage.hpp>
#include <column_operators/Normal/miscellaneous.tcc>

namespace ahead {
    namespace bat {
        namespace ops {

            namespace Private {

                /**
                 * Group-By the tail (generic variant)
                 * @param bat the input BAT
                 * @param AOID the A to encode the new OIDs
                 * @param AOIDinv the inverse of A
                 * @param AHR when reencoding, the A for head
                 * @param AHinvR when reencoding, the inverse for head
                 * @param ATR when reencoding, the A for tail
                 * @param ATinvR when reencoding, the inverse for tail
                 * @return a 4-tuple:
                 * 1) BAT Mapping: (V)OID -> GroupID
                 * 2) BAT Mapping: GroupID -> Value
                 * 3) Bitmap Head: position -> error detected (true) or not (false)
                 * 4) Bitmap Tail: position -> error detected (true) or not (false)
                 */
                template<typename Head, typename Tail, bool reencode>
                struct GroupByAN {

                    typedef typename Head::type_t head_t;
                    typedef typename Tail::type_t tail_t;
                    typedef typename TypeMap<Tail>::v2_base_t::type_t tunenc_t;

                    std::tuple<TempBAT<Head, v2_resoid_t>*, TempBAT<v2_void_t, Tail>*, std::vector<bool>*, std::vector<bool>*> operator()(BAT<Head, Tail>* bat, resoid_t AOID, resoid_t AOIDinv,
                            head_t AHR = 1, head_t AHinvR = 1, tail_t ATR = 1, tail_t ATinvR = 1) const {

                        head_t HAInv = static_cast<head_t>(bat->head.metaData.AN_Ainv);
                        head_t HUnencMaxU = static_cast<head_t>(bat->head.metaData.AN_unencMaxU);
                        tail_t TA = static_cast<tail_t>(bat->tail.metaData.AN_A);
                        tail_t TAInv = static_cast<tail_t>(bat->tail.metaData.AN_Ainv);
                        tail_t TUnencMaxU = static_cast<tail_t>(bat->tail.metaData.AN_unencMaxU);

                        google::dense_hash_map<tunenc_t, oid_t> dictionary;
                        dictionary.set_empty_key(Tail::dhm_emptykey);
                        const bool isHeadEncoded = std::is_base_of<v2_anencoded_t, Head>::value;
                        const bool isTailEncoded = std::is_base_of<v2_anencoded_t, Tail>::value;
                        std::vector<bool> *vec1 = (isHeadEncoded ? new std::vector<bool>(bat->size()) : nullptr);
                        std::vector<bool> *vec2 = (isTailEncoded ? new std::vector<bool>(bat->size()) : nullptr);
                        auto batHeadtoGID = skeletonHead<Head, v2_resoid_t>(bat);
                        batHeadtoGID->tail.metaData = ColumnMetaData(sizeof(resoid_t), AOID, AOIDinv, v2_resoid_t::UNENC_MAX_U, v2_resoid_t::UNENC_MIN);
                        auto batGIDtoTail = skeletonTail<v2_void_t, Tail>(bat);
                        if (reencode) {
                            batHeadtoGID->head.metaData.AN_A = AHR;
                            batHeadtoGID->head.metaData.AN_Ainv = AHinvR;
                            batGIDtoTail->tail.metaData.AN_A = ATR;
                            batGIDtoTail->tail.metaData.AN_Ainv = ATinvR;
                        }
                        auto iter = bat->begin();
                        size_t nextGID = 0;
                        for (size_t i = 0; iter->hasNext(); ++*iter, ++i) {
                            head_t h = isHeadEncoded ? static_cast<head_t>(iter->head() * HAInv) : iter->head();
                            if (isHeadEncoded && (h > HUnencMaxU)) {
                                (*vec1)[i] = true;
                            }
                            if (reencode) {
                                h *= AHR;
                            }
                            tail_t t1 = isTailEncoded ? static_cast<tail_t>(iter->tail() * TAInv) : iter->tail();
                            if (isTailEncoded && (t1 > TUnencMaxU)) {
                                (*vec2)[i] = true;
                            }
                            auto t2 = static_cast<tunenc_t>(t1); // use (potentially smaller) unencoded size
                            // search this tail in our mapping
                            // idx is the void value of mapGIDtoTail, which starts at zero
                            auto iterDict = dictionary.find(t2);
                            if (iterDict == dictionary.end()) {
                                batGIDtoTail->append(reencode ? (t1 * ATR) : isTailEncoded ? (t1 * TA) : t1);
                                batHeadtoGID->append(std::make_pair(h, nextGID * AOID));
                                dictionary[t2] = nextGID;
                                ++nextGID;
                            } else {
                                batHeadtoGID->append(std::make_pair(h, iterDict->second * AOID));
                            }
                        }
                        delete iter;
                        return std::make_tuple(batHeadtoGID, batGIDtoTail, vec1, vec2);
                    }
                };

                /**
                 * Group-By the tail (string specialization)
                 * @param bat the input BAT
                 * @param AOID the A to encode the new OIDs
                 * @param AOIDinv the inverse of A
                 * @param AHR when reencoding, the A for head
                 * @param AHinvR when reencoding, the inverse for head
                 * @param ATR unused for strings
                 * @param ATinvR unused for strings
                 * @return a 4-tuple:
                 * 1) BAT Mapping: (V)OID -> GroupID
                 * 2) BAT Mapping: GroupID -> Value
                 * 3) Bitmap Head: position -> error detected (true) or not (false)
                 * 4) Bitmap Tail: position -> error detected (true) or not (false)
                 */
                template<typename Head, bool reencode>
                struct GroupByAN<Head, v2_str_t, reencode> {

                    typedef typename TypeMap<Head>::v2_encoded_t HEnc;
                    typedef typename TypeMap<v2_str_t>::v2_encoded_t TEnc;
                    typedef typename HEnc::type_t head_t;
                    typedef typename TEnc::type_t tail_t;

                    std::tuple<BAT<Head, v2_resoid_t>*, BAT<v2_void_t, v2_str_t>*, std::vector<bool>*, std::vector<bool>*> operator()(BAT<Head, v2_str_t>* bat, typename v2_resoid_t::type_t AOID,
                            typename v2_resoid_t::type_t AOIDinv, head_t AHR = 1, head_t AHinvR = 1, __attribute__ ((unused)) str_t ATR = nullptr,
                            __attribute__ ((unused)) str_t ATinvR = nullptr) const {

                        head_t HAInv = bat->head.metaData.AN_Ainv;
                        head_t HUnencMaxU = bat->head.metaData.AN_unencMaxU;

                        google::dense_hash_map<str_t, oid_t, hashstr, eqstr> dictionary;
                        dictionary.set_empty_key(v2_str_t::dhm_emptykey);
                        const bool isHeadEncoded = std::is_base_of<v2_anencoded_t, Head>::value;
                        std::vector<bool> *vec1 = (isHeadEncoded ? new std::vector<bool>(bat->size()) : nullptr);
                        auto batHeadtoGID = skeletonHead<Head, v2_resoid_t>(bat);
                        batHeadtoGID->tail.metaData = ColumnMetaData(sizeof(resoid_t), AOID, AOIDinv, v2_resoid_t::UNENC_MAX_U, v2_resoid_t::UNENC_MIN);
                        auto batGIDtoTail = skeletonTail<v2_void_t, v2_str_t>(bat);
                        if (reencode) {
                            batHeadtoGID->head.metaData.AN_A = AHR;
                            batHeadtoGID->head.metaData.AN_Ainv = AHinvR;
                        }
                        auto iter = bat->begin();
                        size_t nextGID = 0;
                        for (size_t i = 0; iter->hasNext(); ++*iter, ++i) {
                            head_t h = isHeadEncoded ? static_cast<head_t>(iter->head() * HAInv) : iter->head();
                            if (isHeadEncoded && (h > HUnencMaxU)) {
                                (*vec1)[i] = true;
                            }
                            if (reencode) {
                                h *= AHR;
                            }
                            auto t = iter->tail();
                            // search this tail in our mapping
                            // idx is the void value of mapGIDtoTail, which starts at zero
                            auto iterDict = dictionary.find(t);
                            if (iterDict == dictionary.end()) {
                                batGIDtoTail->append(t);
                                batHeadtoGID->append(std::make_pair(h, nextGID * AOID));
                                dictionary[t] = nextGID;
                                ++nextGID;
                            } else {
                                batHeadtoGID->append(std::make_pair(h, iterDict->second * AOID));
                            }
                        }
                        delete iter;
                        return make_tuple(batHeadtoGID, batGIDtoTail, vec1, nullptr);
                    }
                };
            }

            /**
             * The actual Group By operator
             * @param bat the input BAT
             * @param AOID the A to encode the new OIDs
             * @param AOIDinv the inverse of A
             * @return 4-tuple:
             * 1) BAT Mapping: (V)OID -> GroupID
             * 2) BAT Mapping: GroupID -> Value
             * 3) Bitmap Head: position -> error detected (true) or not (false)
             * 4) Bitmap Tail: position -> error detected (true) or not (false)
             */
            template<typename Head, typename Tail>
            std::tuple<BAT<Head, v2_resoid_t>*, BAT<v2_void_t, Tail>*, std::vector<bool>*, std::vector<bool>*> groupbyAN(BAT<Head, Tail>* bat,
                    typename v2_resoid_t::type_t AOID = std::get<ANParametersSelector<v2_resoid_t>::As->size() - 1>(*ANParametersSelector<v2_resoid_t>::As), // use largest A for encoding by default
                    typename v2_resoid_t::type_t AOIDinv = std::get<ANParametersSelector<v2_resoid_t>::Ainvs->size() - 1>(*ANParametersSelector<v2_resoid_t>::Ainvs) // and the appropriate inverse
                            ) {
                return Private::GroupByAN<Head, Tail, false>()(bat, AOID, AOIDinv);
            }

            /**
             * The actual Group By operator
             * @param bat the input BAT
             * @param AOID the A to encode the new OIDs
             * @param AOIDinv the inverse of A
             * @param AHR when reencoding, the A for head
             * @param AHinvR when reencoding, the inverse for head
             * @param ATR when reencoding, the A for tail
             * @param ATinvR when reencoding, the inverse for tail
             * @return 4-tuple:
             * 1) BAT Mapping: (V)OID -> GroupID
             * 2) BAT Mapping: GroupID -> Value
             * 3) Bitmap Head: position -> error detected (true) or not (false)
             * 4) Bitmap Tail: position -> error detected (true) or not (false)
             */
            template<typename Head, typename Tail>
            std::tuple<BAT<Head, v2_resoid_t>*, BAT<v2_void_t, Tail>*, std::vector<bool>*, std::vector<bool>*> groupbyAN(BAT<Head, Tail>* bat, typename Head::type_t AHR, typename Head::type_t AHinvR,
                    typename Tail::type_t ATR, typename Tail::type_t ATinvR,
                    typename v2_resoid_t::type_t AOID = std::get<ANParametersSelector<v2_resoid_t>::As->size() - 1>(*ANParametersSelector<v2_resoid_t>::As), // use largest A for encoding by default
                    typename v2_resoid_t::type_t AOIDinv = std::get<ANParametersSelector<v2_resoid_t>::Ainvs->size() - 1>(*ANParametersSelector<v2_resoid_t>::Ainvs) // and the appropriate inverse
                            ) {
                return Private::GroupByAN<Head, Tail, true>()(bat, AOID, AOIDinv, AHR, AHinvR, ATR, ATinvR);
            }

            namespace Private {

                /**
                 * Group by 2 BAT's and sum up one column according to the double-grouping. The OID's (Heads) in all BAT's must match!
                 * @param bat1 The bat over which to sum up
                 * @param bat2 The first grouping BAT
                 * @param bat3 The second grouping BAT
                 * @return Five BATs and six bitmaps:
                 * 1) sum over double group-by.
                 * 2) Mapping sum (V)OID -> group1 RESOID.
                 * 3) Mapping group1 (V)OID -> group1 value.
                 * 4) Mapping sum (V)OID -> group2 RESOID.
                 * 5) Mapping group2 (V)OID -> group2 value
                 * 6) Bitmap Head1: position -> error detected (true) or not (false)
                 * 7) Bitmap Tail1: position -> error detected (true) or not (false)
                 * 8) Bitmap Head2: position -> error detected (true) or not (false)
                 * 9) Bitmap Tail2: position -> error detected (true) or not (false)
                 * 10) Bitmap Head3: position -> error detected (true) or not (false)
                 * 11) Bitmap Tail3: position -> error detected (true) or not (false)
                 */
                template<typename V2Result, typename Head1, typename Tail1, typename Head2, typename Tail2, typename Head3, typename Tail3, bool reencode>
                struct GroupedSumAN {

                    std::tuple<BAT<v2_void_t, V2Result>*, BAT<v2_void_t, v2_resoid_t>*, BAT<v2_void_t, Tail2>*, BAT<v2_void_t, v2_resoid_t>*, BAT<v2_void_t, Tail3>*, std::vector<bool>*,
                            std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> operator()(BAT<Head1, Tail1>* bat1, BAT<Head2, Tail2>* bat2,
                            BAT<Head3, Tail3>* bat3, typename Head2::type_t AH2R = 1, typename Head2::type_t AH2invR = 1, typename Tail2::type_t AT2R = 1, typename Tail2::type_t AT2invR = 1,
                            typename Head3::type_t AH3R = 1, typename Head3::type_t AH3invR = 1, typename Tail3::type_t AT3R = 1, typename Tail3::type_t AT3invR = 1,
                            typename v2_resoid_t::type_t AOID = std::get<ANParametersSelector<v2_resoid_t>::As->size() - 1>(*ANParametersSelector<v2_resoid_t>::As), // use largest A for encoding by default
                            typename v2_resoid_t::type_t AOIDinv = std::get<ANParametersSelector<v2_resoid_t>::Ainvs->size() - 1>(*ANParametersSelector<v2_resoid_t>::Ainvs),
                            typename V2Result::type_t ARes = std::get<ANParametersSelector<V2Result>::As->size() - 1>(*ANParametersSelector<v2_resoid_t>::As), // use largest A for encoding by default
                            typename v2_resoid_t::type_t AResInv = std::get<ANParametersSelector<V2Result>::Ainvs->size() - 1>(*ANParametersSelector<v2_resoid_t>::Ainvs)) {
                        static_assert(std::is_base_of<v2_anencoded_t, V2Result>::value, "V2Result is not derived from v2_anencoded_t!");

                        typedef typename Head1::type_t head1_t;
                        typedef typename Tail1::type_t tail1_t;
                        head1_t H1AInv = bat1->head.metaData.AN_Ainv;
                        head1_t H1UnencMaxU = bat1->head.metaData.AN_unencMaxU;
                        tail1_t T1AInv = bat1->tail.metaData.AN_Ainv;
                        tail1_t T1UnencMaxU = bat1->tail.metaData.AN_unencMaxU;

#ifdef DEBUG
                        StopWatch sw;
                        sw.start();
#endif
                        auto group1 = reencode ? groupbyAN(bat2, AH2R, AH2invR, AT2R, AT2invR) : groupbyAN(bat2);
#ifdef DEBUG
                        auto time1 = sw.stop();
                        sw.start();
#endif
                        auto group2 = reencode ? groupbyAN(bat3, AH3R, AH3invR, AT3R, AT3invR) : groupbyAN(bat3);
#ifdef DEBUG
                        auto time2 = sw.stop();
                        sw.start();
#endif
                        // create an array which can hold enough sums
                        auto size1 = std::get<1>(group1)->size();
                        auto size2 = std::get<1>(group2)->size();
                        auto numgroups = size1 * size2;

                        typedef typename TempBAT<v2_void_t, v2_resoid_t>::coldesc_head_t cd_void_t;
                        typedef typename TempBAT<v2_void_t, V2Result>::coldesc_tail_t cd_result_t;
                        auto sumBat = new TempBAT<v2_void_t, V2Result>(cd_void_t(),
                                cd_result_t(ColumnMetaData(sizeof(typename V2Result::type_t), ARes, AResInv, V2Result::UNENC_MAX_U, V2Result::UNENC_MIN)));
                        sumBat->tail.container->resize(numgroups, 0);
                        typedef typename TempBAT<v2_void_t, v2_resoid_t>::coldesc_tail_t cd_resoid_t;
                        auto outBat2 = new TempBAT<v2_void_t, v2_resoid_t>(cd_void_t(),
                                cd_resoid_t(ColumnMetaData(sizeof(typename v2_resoid_t::type_t), AOID, AOIDinv, v2_resoid_t::UNENC_MAX_U, v2_resoid_t::UNENC_MIN)));
                        outBat2->reserve(numgroups);
                        auto outBat4 = new TempBAT<v2_void_t, v2_resoid_t>(cd_void_t(),
                                cd_resoid_t(ColumnMetaData(sizeof(typename v2_resoid_t::type_t), AOID, AOIDinv, v2_resoid_t::UNENC_MAX_U, v2_resoid_t::UNENC_MIN)));
                        outBat4->reserve(numgroups);

                        auto g1SecondIter = std::get<1>(group1)->begin();
                        auto g2SecondIter = std::get<1>(group2)->begin();
                        for (; g1SecondIter->hasNext(); ++*g1SecondIter) {
                            g2SecondIter->position(0);
                            for (; g2SecondIter->hasNext(); ++*g2SecondIter) {
                                outBat2->append(g1SecondIter->head() * AOID); // encode mapping OIDs
                                outBat4->append(g2SecondIter->head() * AOID); // encode mapping OIDs
                            }
                        }

                        const bool isHead1Encoded = std::is_base_of<v2_anencoded_t, Head1>::value;
                        const bool isTail1Encoded = std::is_base_of<v2_anencoded_t, Tail1>::value;
                        std::vector<bool> *vec1 = (isHead1Encoded ? new std::vector<bool>(bat1->size()) : nullptr);
                        std::vector<bool> *vec2 = (isTail1Encoded ? new std::vector<bool>(bat1->size()) : nullptr);

                        auto sums = sumBat->tail.container->data();
                        auto iter1 = bat1->begin();
                        auto g1FirstIter = std::get<0>(group1)->begin();
                        v2_resoid_t::type_t AG1Inv = std::get<0>(group1)->tail.metaData.AN_Ainv;
                        auto g2FirstIter = std::get<0>(group2)->begin();
                        v2_resoid_t::type_t AG2Inv = std::get<0>(group2)->tail.metaData.AN_Ainv;
#ifdef DEBUG
                        auto iter2 = bat2->begin();
                        auto iter3 = bat3->begin();
                        std::cerr << "+------------+--------+-----------+\n";
                        std::cerr << "| lo_revenue | d_year | p_brand   |\n";
                        std::cerr << "+============+========+===========+\n";
                        for (size_t i = 0; iter1->hasNext(); ++*iter1, ++*iter2, ++*iter3, ++*g1FirstIter, ++*g2FirstIter, ++i) {
#else
                        for (size_t i = 0; iter1->hasNext(); ++*iter1, ++*g1FirstIter, ++*g2FirstIter, ++i) {
#endif
#ifdef DEBUG
                            std::cerr << "| " << std::setw(10) << iter1->tail();
                            std::cerr << " | " << std::setw(6) << iter2->tail();
                            std::cerr << " | " << std::setw(9) << iter3->tail() << " |\n";
                            // g1SecondIter->position(g1FirstIter->tail());
                            // std::cerr << std::setw(6) << g1SecondIter->tail() << " | ";
                            // g2SecondIter->position(g2FirstIter->tail());
                            // std::cerr << std::setw(9) << g2SecondIter->tail() << " |\n";
#endif
                            if (isHead1Encoded && (static_cast<head1_t>(iter1->head() * H1AInv) > H1UnencMaxU)) {
                                (*vec1)[i] = true;
                            }
                            auto curTail = isTail1Encoded ? static_cast<tail1_t>(iter1->tail() * T1AInv) : iter1->tail();
                            if (isTail1Encoded && (curTail > T1UnencMaxU)) {
                                (*vec2)[i] = true;
                            }
                            size_t pos = (g1FirstIter->tail() * AG1Inv) * size2 + (g2FirstIter->tail() * AG2Inv);
                            sums[pos] += static_cast<typename V2Result::type_t>(curTail * ARes);
                        }
#ifdef DEBUG
                        delete iter3;
                        delete iter2;
                        std::cerr << "+------------+--------+-----------+";
#endif
                        delete iter1;
                        delete g1FirstIter;
                        delete g2FirstIter;
                        delete g1SecondIter;
                        delete g2SecondIter;
                        delete std::get<0>(group1);
                        delete std::get<0>(group2);
#ifdef DEBUG
                        auto time3 = sw.stop();
                        std::cout << "group1 took " << time1 << " ns. group2 took " << time2 << " ns. grouped sum took " << time3 << " ns." << endl;
#endif
                        return make_tuple(sumBat, outBat2, std::get<1>(group1), outBat4, std::get<1>(group2), vec1, vec2, std::get<2>(group1), std::get<3>(group1), std::get<2>(group2),
                                std::get<3>(group2));
                    }
                };
            }

            template<typename V2Result, typename Head1, typename Tail1, typename Head2, typename Tail2, typename Head3, typename Tail3>
            std::tuple<BAT<v2_void_t, V2Result>*, BAT<v2_void_t, v2_resoid_t>*, BAT<v2_void_t, Tail2>*, BAT<v2_void_t, v2_resoid_t>*, BAT<v2_void_t, Tail3>*, std::vector<bool>*, std::vector<bool>*,
                    std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> groupedSumAN(BAT<Head1, Tail1>* bat1, BAT<Head2, Tail2>* bat2, BAT<Head3, Tail3>* bat3,
                    typename v2_resoid_t::type_t AOID = std::get<ANParametersSelector<v2_resoid_t>::As->size() - 1>(*ANParametersSelector<v2_resoid_t>::As), // use largest A for encoding by default
                    typename v2_resoid_t::type_t AOIDinv = std::get<ANParametersSelector<v2_resoid_t>::Ainvs->size() - 1>(*ANParametersSelector<v2_resoid_t>::Ainvs), typename V2Result::type_t ARes =
                            std::get<ANParametersSelector<V2Result>::As->size() - 1>(*ANParametersSelector<v2_resoid_t>::As), // use largest A for encoding by default
                    typename v2_resoid_t::type_t AResInv = std::get<ANParametersSelector<V2Result>::Ainvs->size() - 1>(*ANParametersSelector<v2_resoid_t>::Ainvs)) {
                return Private::GroupedSumAN<V2Result, Head1, Tail1, Head2, Tail2, Head3, Tail3, false>()(bat1, bat2, bat3, 0, 0, 0, 0, 0, 0, 0, 0, AOID, AOIDinv, ARes, AResInv);
            }

            template<typename V2Result, typename Head1, typename Tail1, typename Head2, typename Tail2, typename Head3, typename Tail3>
            std::tuple<BAT<v2_void_t, V2Result>*, BAT<v2_void_t, v2_resoid_t>*, BAT<v2_void_t, Tail2>*, BAT<v2_void_t, v2_resoid_t>*, BAT<v2_void_t, Tail3>*, std::vector<bool>*, std::vector<bool>*,
                    std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> groupedSumAN(BAT<Head1, Tail1>* bat1, BAT<Head2, Tail2>* bat2, BAT<Head3, Tail3>* bat3,
                    typename Head2::type_t AH2R, typename Head2::type_t AH2invR, typename Tail2::type_t AT2R, typename Tail2::type_t AT2invR, typename Head3::type_t AH3R,
                    typename Head3::type_t AH3invR, typename Tail3::type_t AT3R, typename Tail3::type_t AT3invR,
                    typename v2_resoid_t::type_t AOID = std::get<ANParametersSelector<v2_resoid_t>::As->size() - 1>(*ANParametersSelector<v2_resoid_t>::As), // use largest A for encoding by default
                    typename v2_resoid_t::type_t AOIDinv = std::get<ANParametersSelector<v2_resoid_t>::Ainvs->size() - 1>(*ANParametersSelector<v2_resoid_t>::Ainvs), typename V2Result::type_t ARes =
                            std::get<ANParametersSelector<V2Result>::As->size() - 1>(*ANParametersSelector<v2_resoid_t>::As), // use largest A for encoding by default
                    typename v2_resoid_t::type_t AResInv = std::get<ANParametersSelector<V2Result>::Ainvs->size() - 1>(*ANParametersSelector<v2_resoid_t>::Ainvs)) {
                return Private::GroupedSumAN<V2Result, Head1, Tail1, Head2, Tail2, Head3, Tail3, true>()(bat1, bat2, bat3, AH2R, AH2invR, AT2R, AT2invR, AH3R, AH3invR, AT3R, AT3invR, AOID, AOIDinv,
                        ARes, AResInv);
            }
        }
    }
}

#endif /* GROUPBY_AN_TCC */
