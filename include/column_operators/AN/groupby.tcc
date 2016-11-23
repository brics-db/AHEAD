// Copyright (c) 2016 Till Kolditz
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
#endif
#include <type_traits>

#include <util/resilience.hpp>

namespace v2 {
    namespace bat {
        namespace ops {

            namespace Private {

                /**
                 * Group-By the tail (generic variant)
                 * Returns a 4-tuple:
                 * 1) BAT Mapping: (V)OID -> GroupID
                 * 2) BAT Mapping: GroupID -> Value
                 * 3) Bitmap Head: position -> error detected (true) or not (false)
                 * 4) Bitmap Tail: position -> error detected (true) or not (false)
                 */
                template<typename Head, typename Tail>
                struct groupbyAN {
                    typedef typename Tail::type_t tail_t;
                    typedef typename TypeMap<Head>::v2_encoded_t HEnc;
                    typedef typename TypeMap<Tail>::v2_encoded_t TEnc;
                    typedef typename HEnc::type_t henc_t;
                    typedef typename TEnc::type_t tenc_t;

                    tuple<Bat<Head, v2_oid_t>*, Bat<v2_void_t, Tail>*, vector<bool>*, vector<bool>*>
                    operator()(
                            Bat<Head, Tail>* bat,
                            henc_t AHinv = HEnc::A_INV,
                            henc_t AHunencMaxU = HEnc::A_UNENC_MAX_U,
                            tenc_t ATinv = TEnc::A_INV,
                            tenc_t ATunencMaxU = TEnc::A_UNENC_MAX_U,
                            __attribute__((unused)) typename v2_resoid_t::type_t AOID = v2_resoid_t::A
                            ) const {
                        google::dense_hash_map<tail_t, oid_t> dictionary;
                        dictionary.set_empty_key(Tail::dhm_emptykey);
                        const bool isHeadEncoded = is_base_of<v2_anencoded_t, Head>::value;
                        const bool isTailEncoded = is_base_of<v2_anencoded_t, Tail>::value;
                        vector<bool> *vec1 = (isHeadEncoded ? new vector<bool>(bat->size()) : nullptr);
                        vector<bool> *vec2 = (isTailEncoded ? new vector<bool>(bat->size()) : nullptr);
                        auto batHeadtoGID = new TempBat<Head, v2_oid_t>();
                        auto batGIDtoTail = new TempBat<v2_void_t, Tail>();
                        auto iter = bat->begin();
                        size_t nextGID = 0;
                        for (size_t i = 0; iter->hasNext(); ++*iter, ++i) {
                            if (isHeadEncoded && ((iter->head() * AHinv) > AHunencMaxU)) {
                                (*vec1)[i] = true;
                            }
                            auto curTail = iter->tail();
                            if (isTailEncoded && ((curTail * ATinv) > ATunencMaxU)) {
                                (*vec2)[i] = true;
                            }
                            // search this tail in our mapping
                            // idx is the void value of mapGIDtoTail, which starts at zero
                            auto iterDict = dictionary.find(curTail);
                            if (iterDict == dictionary.end()) {
                                batGIDtoTail->append(curTail);
                                batHeadtoGID->append(make_pair(iter->head(), nextGID));
                                dictionary[curTail] = nextGID;
                                ++nextGID;
                            } else {
                                batHeadtoGID->append(make_pair(iter->head(), iterDict->second));
                            }
                        }
                        delete iter;
                        return make_tuple(batHeadtoGID, batGIDtoTail, vec1, vec2);
                    }
                };

                /**
                 * Group-By the tail (string specialization)
                 * Returns a 4-tuple:
                 * 1) BAT Mapping: (V)OID -> GroupID
                 * 2) BAT Mapping: GroupID -> Value
                 * 3) Bitmap Head: position -> error detected (true) or not (false)
                 * 4) Bitmap Tail: position -> error detected (true) or not (false)
                 */
                template<typename Head>
                struct groupbyAN<Head, v2_str_t> {
                    typedef typename TypeMap<Head>::v2_encoded_t HEnc;
                    typedef typename TypeMap<v2_str_t>::v2_encoded_t TEnc;
                    typedef typename HEnc::type_t henc_t;
                    typedef typename TEnc::type_t tenc_t;

                    tuple<Bat<Head, v2_oid_t>*, Bat<v2_void_t, v2_str_t>*, vector<bool>*, vector<bool>*>
                    operator()(
                            Bat<Head, v2_str_t>* bat,
                            henc_t AHinv = HEnc::A_INV,
                            henc_t AHunencMaxU = HEnc::A_UNENC_MAX_U,
                            __attribute__((unused)) tenc_t ATinv = TEnc::A_INV,
                            __attribute__((unused)) tenc_t ATunencMaxU = TEnc::A_UNENC_MAX_U,
                            __attribute__((unused)) typename v2_resoid_t::type_t AOID = v2_resoid_t::A
                            ) const {
                        google::dense_hash_map<str_t, oid_t, hashstr, eqstr> dictionary;
                        dictionary.set_empty_key(v2_str_t::dhm_emptykey);
                        const bool isHeadEncoded = is_base_of<v2_anencoded_t, Head>::value;
                        vector<bool> *vec1 = (isHeadEncoded ? new vector<bool>(bat->size()) : nullptr);
                        auto batHeadtoGID = new TempBat<Head, v2_oid_t>();
                        auto batGIDtoTail = new TempBat<v2_void_t, v2_str_t>();
                        auto iter = bat->begin();
                        size_t nextGID = 0;
                        for (size_t i = 0; iter->hasNext(); ++*iter, ++i) {
                            if (isHeadEncoded && ((iter->head() * AHinv) > AHunencMaxU)) {
                                (*vec1)[i] = true;
                            }
                            auto curTail = iter->tail();
                            // search this tail in our mapping
                            // idx is the void value of mapGIDtoTail, which starts at zero
                            auto iterDict = dictionary.find(curTail);
                            if (iterDict == dictionary.end()) {
                                batGIDtoTail->append(curTail);
                                batHeadtoGID->append(make_pair(iter->head(), nextGID));
                                dictionary[curTail] = nextGID;
                                ++nextGID;
                            } else {
                                batHeadtoGID->append(make_pair(iter->head(), iterDict->second));
                            }
                        }
                        delete iter;
                        return make_tuple(batHeadtoGID, batGIDtoTail, vec1, nullptr);
                    }
                };
            }

            /**
             * The actual Group By operator
             * @param bat
             * @return 2 BAT's:
             * 1) Mapping (V)OID -> GroupID
             * 2) GroupID -> Value
             * 3) Bitmap Head: position -> error detected (true) or not (false)
             * 4) Bitmap Tail: position -> error detected (true) or not (false)
             */
            template<typename Head, typename Tail>
            tuple<Bat<Head, v2_oid_t>*, Bat<v2_void_t, Tail>*, vector<bool>*, vector<bool>*>
            groupbyAN(
                    Bat<Head, Tail>* bat,
                    typename TypeMap<Head>::v2_encoded_t::type_t AHinv = TypeMap<Head>::v2_encoded_t::A_INV,
                    typename TypeMap<Head>::v2_encoded_t::type_t AHunencMaxU = TypeMap<Head>::v2_encoded_t::A_UNENC_MAX_U,
                    typename TypeMap<Tail>::v2_encoded_t::type_t ATinv = TypeMap<Tail>::v2_encoded_t::A_INV,
                    typename TypeMap<Tail>::v2_encoded_t::type_t ATunencMaxU = TypeMap<Tail>::v2_encoded_t::A_UNENC_MAX_U
                    ) {
                return Private::groupbyAN<Head, Tail>()(bat, AHinv, AHunencMaxU, ATinv, ATunencMaxU);
            }

            /**
             * Group by 2 BAT's and sum up one column according to the double-grouping. The OID's (Heads) in all BAT's must match!
             * @param bat1 The bat over which to sum up
             * @param bat2 The first grouping BAT
             * @param bat3 The second grouping BAT
             * @return Five BATs: 1) sum over double group-by.
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
            template<typename V2Result, typename Head1, typename Tail1, typename Head2, typename Tail2, typename Head3, typename Tail3>
            tuple<Bat<v2_void_t, V2Result>*, Bat<v2_void_t, v2_resoid_t>*, Bat<v2_void_t, Tail2>*, Bat<v2_void_t, v2_resoid_t>*, Bat<v2_void_t, Tail3>*, vector<bool>*, vector<bool>*, vector<bool>*, vector<bool>*, vector<bool>*, vector<bool>*>
            groupedSumAN(
                    Bat<Head1, Tail1>* bat1,
                    Bat<Head2, Tail2>* bat2,
                    Bat<Head3, Tail3>* bat3,
                    // typename TypeMap<Head1>::v2_encoded_t::type_t AH1 = TypeMap<Head1>::v2_encoded_t::A,
                    typename TypeMap<Head1>::v2_encoded_t::type_t AH1inv = TypeMap<Head1>::v2_encoded_t::A_INV,
                    typename TypeMap<Head1>::v2_encoded_t::type_t AH1unencMaxU = TypeMap<Head1>::v2_encoded_t::A_UNENC_MAX_U,
                    // typename TypeMap<Tail1>::v2_encoded_t::type_t AT1 = TypeMap<Tail1>::v2_encoded_t::A,
                    typename TypeMap<Tail1>::v2_encoded_t::type_t AT1inv = TypeMap<Tail1>::v2_encoded_t::A_INV,
                    typename TypeMap<Tail1>::v2_encoded_t::type_t AT1unencMaxU = TypeMap<Tail1>::v2_encoded_t::A_UNENC_MAX_U,
                    // typename TypeMap<Head2>::v2_encoded_t::type_t AH2 = TypeMap<Head2>::v2_encoded_t::A,
                    typename TypeMap<Head2>::v2_encoded_t::type_t AH2inv = TypeMap<Head2>::v2_encoded_t::A_INV,
                    typename TypeMap<Head2>::v2_encoded_t::type_t AH2unencMaxU = TypeMap<Head2>::v2_encoded_t::A_UNENC_MAX_U,
                    // typename TypeMap<Tail2>::v2_encoded_t::type_t AT2 = TypeMap<Tail2>::v2_encoded_t::A,
                    typename TypeMap<Tail2>::v2_encoded_t::type_t AT2inv = TypeMap<Tail2>::v2_encoded_t::A_INV,
                    typename TypeMap<Tail2>::v2_encoded_t::type_t AT2unencMaxU = TypeMap<Tail2>::v2_encoded_t::A_UNENC_MAX_U,
                    // typename TypeMap<Head3>::v2_encoded_t::type_t AH3 = TypeMap<Head3>::v2_encoded_t::A,
                    typename TypeMap<Head3>::v2_encoded_t::type_t AH3inv = TypeMap<Head3>::v2_encoded_t::A_INV,
                    typename TypeMap<Head3>::v2_encoded_t::type_t AH3unencMaxU = TypeMap<Head3>::v2_encoded_t::A_UNENC_MAX_U,
                    // typename TypeMap<Tail3>::v2_encoded_t::type_t AT3 = TypeMap<Tail3>::v2_encoded_t::A,
                    typename TypeMap<Tail3>::v2_encoded_t::type_t AT3inv = TypeMap<Tail3>::v2_encoded_t::A_INV,
                    typename TypeMap<Tail3>::v2_encoded_t::type_t AT3unencMaxU = TypeMap<Tail3>::v2_encoded_t::A_UNENC_MAX_U,
                    typename v2_resoid_t::type_t AOID = v2_resoid_t::A
                    ) {
#ifdef DEBUG
                StopWatch sw;
                sw.start();
#endif
                auto group1 = groupbyAN(bat2, AH2inv, AH2unencMaxU, AT2inv, AT2unencMaxU);
#ifdef DEBUG
                auto time1 = sw.stop();
                sw.start();
#endif
                auto group2 = groupbyAN(bat3, AH3inv, AH3unencMaxU, AT3inv, AT3unencMaxU);
#ifdef DEBUG
                auto time2 = sw.stop();
                sw.start();
#endif
                // create an array which can hold enough sums
                auto size1 = get<1>(group1)->size();
                auto size2 = get<1>(group2)->size();
                auto numgroups = size1 * size2;

                auto sumBat = new TempBat<v2_void_t, V2Result>();
                sumBat->tail.container->resize(numgroups, 0);
                auto outBat2 = new TempBat<v2_void_t, v2_resoid_t>(numgroups);
                auto outBat4 = new TempBat<v2_void_t, v2_resoid_t>(numgroups);

                auto g1SecondIter = get<1>(group1)->begin();
                auto g2SecondIter = get<1>(group2)->begin();
                for (; g1SecondIter->hasNext(); ++*g1SecondIter) {
                    g2SecondIter->position(0);
                    for (; g2SecondIter->hasNext(); ++*g2SecondIter) {
                        outBat2->append(g1SecondIter->head() * AOID); // encode mapping OIDs
                        outBat4->append(g2SecondIter->head() * AOID); // encode mapping OIDs
                    }
                }

                const bool isHead1Encoded = is_base_of<v2_anencoded_t, Head1>::value;
                const bool isTail1Encoded = is_base_of<v2_anencoded_t, Tail1>::value;
                vector<bool> *vec1 = (isHead1Encoded ? new vector<bool>(bat1->size()) : nullptr);
                vector<bool> *vec2 = (isTail1Encoded ? new vector<bool>(bat1->size()) : nullptr);

                auto sums = sumBat->tail.container->data();
                auto iter1 = bat1->begin();
                auto g1FirstIter = get<0>(group1)->begin();
                auto g2FirstIter = get<0>(group2)->begin();
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
                    if (isHead1Encoded && ((iter1->head() * AH1inv) > AH1unencMaxU)) {
                        (*vec1)[i] = true;
                    }
                    auto curTail = iter1->tail();
                    if (isTail1Encoded && ((curTail * AT1inv) > AT1unencMaxU)) {
                        (*vec2)[i] = true;
                    }
                    size_t pos = g1FirstIter->tail() * size2 + g2FirstIter->tail();
                    sums[pos] += static_cast<typename V2Result::type_t> (curTail);
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
                delete get<0>(group1);
                delete get<0>(group2);
#ifdef DEBUG
                auto time3 = sw.stop();
                std::cout << "group1 took " << time1 << " ns. group2 took " << time2 << " ns. grouped sum took " << time3 << " ns." << endl;
#endif
                return make_tuple(sumBat, outBat2, get<1>(group1), outBat4, get<1>(group2), vec1, vec2, get<2>(group1), get<3>(group1), get<2>(group2), get<3>(group2));
            }
        }
    }
}

#endif /* GROUPBY_AN_TCC */
