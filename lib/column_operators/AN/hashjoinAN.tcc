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
 * File:   hashjoinAN.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 22. November 2016, 16:24
 */

#ifndef HASHJOIN_AN_TCC
#define HASHJOIN_AN_TCC

#include <limits>
#include <type_traits>
#include <utility>

#include <google/dense_hash_map>

#include <column_storage/Storage.hpp>
#include <util/v2typeconversion.hpp>
#include <column_operators/ANbase.hpp>
#include "../miscellaneous.hpp"
#include "ANhelper.tcc"

namespace ahead {
    namespace bat {
        namespace ops {
            namespace Hashjoin {

                template<typename T, bool>
                struct ReturnTypeSelector {

                    typedef typename T::v2_select_t v2_select_t;
                };

                template<typename T>
                struct ReturnTypeSelector<T, true> {

                    typedef typename TypeMap<T>::v2_encoded_t::v2_select_t v2_select_t;
                };

                template<typename Head1, typename Tail1, typename Head2, typename Tail2, bool reencodeHead, bool reencodeTail>
                struct hashjoinANunencHashmap {

                    typedef typename ReturnTypeSelector<Head1, reencodeHead>::v2_select_t v2_h1_select_t;
                    typedef typename ReturnTypeSelector<Tail2, reencodeTail>::v2_select_t v2_t2_select_t;
                    typedef typename TypeMap<Head1>::v2_encoded_t H1Enc;
                    typedef typename H1Enc::type_t h1enc_t;
                    typedef typename TypeMap<Tail1>::v2_encoded_t T1Enc;
                    typedef typename T1Enc::type_t t1enc_t;
                    typedef typename TypeMap<Head2>::v2_encoded_t H2Enc;
                    typedef typename H2Enc::type_t h2enc_t;
                    typedef typename TypeMap<Tail2>::v2_encoded_t T2Enc;
                    typedef typename T2Enc::type_t t2enc_t;
                    typedef typename TypeMap<Head1>::v2_base_t::type_t h1unenc_t;
                    typedef typename TypeMap<Tail1>::v2_base_t::type_t t1unenc_t;
                    typedef typename TypeMap<Head2>::v2_base_t::type_t h2unenc_t;
                    typedef typename TypeMap<Tail2>::v2_base_t::type_t t2unenc_t;
                    typedef typename ahead::larger_type<t1unenc_t, h2unenc_t>::type_t larger_t;
                    typedef ANhelper<Head1> head1_helper_t;
                    typedef ANhelper<Tail1> tail1_helper_t;
                    typedef ANhelper<Head2> head2_helper_t;
                    typedef ANhelper<Tail2> tail2_helper_t;

                    static std::tuple<TempBAT<v2_h1_select_t, v2_t2_select_t>*, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *> run(
                            BAT<Head1, Tail1>* arg1,
                            BAT<Head2, Tail2>* arg2,
                            hash_side_t hashside, // by that, by default the order of the left BAT is preserved (what we expect in the queries)
                            resoid_t AOID,
                            h1enc_t __attribute__((unused)) AH1R = 1, // for reencode
                            h1enc_t __attribute__((unused)) AH1InvR = 1, // for reencode
                            t2enc_t __attribute__((unused)) AT2R = 1, // for reencode
                            t2enc_t __attribute__((unused)) AT2InvR = 1 // for reencode
                            ) {
                        const h1enc_t AH1Inv = head1_helper_t::getIfEncoded(arg1->head.metaData.AN_Ainv);
                        const h1enc_t AH1UnencMaxU = arg1->head.metaData.AN_unencMaxU;
                        const t1enc_t AT1Inv = tail1_helper_t::getIfEncoded(arg1->tail.metaData.AN_Ainv);
                        const t1enc_t AT1UnencMaxU = arg1->tail.metaData.AN_unencMaxU;
                        const h2enc_t AH2Inv = head2_helper_t::getIfEncoded(arg2->head.metaData.AN_Ainv);
                        const h2enc_t AH2UnencMaxU = arg2->head.metaData.AN_unencMaxU;
                        const t2enc_t AT2Inv = tail2_helper_t::getIfEncoded(arg2->tail.metaData.AN_Ainv);
                        const t2enc_t AT2UnencMaxU = arg2->tail.metaData.AN_unencMaxU;
                        // do we need any conversion between left Tail and right Head? If so, also regard which of the types is larger
                        const h1enc_t __attribute__((unused)) reencFactorH1 = head1_helper_t::mulIfEncoded(AH1R, AH1Inv);
                        const t2enc_t __attribute__((unused)) reencFactorT2 = tail2_helper_t::mulIfEncoded(AT2R, AT2Inv);
                        TempBAT<v2_h1_select_t, v2_t2_select_t> * bat = nullptr;
                        if constexpr (reencodeHead & reencodeTail) {
                            typedef typename TempBAT<v2_h1_select_t, v2_t2_select_t>::coldesc_head_t bat_coldesc_head_t;
                            typedef typename TempBAT<v2_h1_select_t, v2_t2_select_t>::coldesc_tail_t bat_coldesc_tail_t;
                            bat = new TempBAT<v2_h1_select_t, v2_t2_select_t>(
                                    bat_coldesc_head_t(ColumnMetaData(arg1->head.metaData.width, AH1R, AH1InvR, arg1->head.metaData.AN_unencMaxU, arg1->head.metaData.AN_unencMinS)),
                                    bat_coldesc_tail_t(ColumnMetaData(arg2->tail.metaData.width, AT2R, AT2InvR, arg2->tail.metaData.AN_unencMaxU, arg2->tail.metaData.AN_unencMinS)));
                        } else if constexpr (reencodeHead) {
                            typedef typename TempBAT<v2_h1_select_t, v2_t2_select_t>::coldesc_head_t bat_coldesc_head_t;
                            typedef typename TempBAT<v2_h1_select_t, v2_t2_select_t>::coldesc_tail_t bat_coldesc_tail_t;
                            auto & tmd = arg2->tail.metaData;
                            bat = new TempBAT<v2_h1_select_t, v2_t2_select_t>(
                                    bat_coldesc_head_t(ColumnMetaData(arg1->head.metaData.width, AH1R, AH1InvR, arg1->head.metaData.AN_unencMaxU, arg1->head.metaData.AN_unencMinS)),
                                    bat_coldesc_tail_t(ColumnMetaData(tmd.width, tmd.AN_A, tmd.AN_Ainv, tmd.AN_unencMaxU, tmd.AN_unencMinS)));
                        } else if constexpr (reencodeTail) {
                            typedef typename TempBAT<v2_h1_select_t, v2_t2_select_t>::coldesc_head_t bat_coldesc_head_t;
                            typedef typename TempBAT<v2_h1_select_t, v2_t2_select_t>::coldesc_tail_t bat_coldesc_tail_t;
                            auto & hmd = arg1->head.metaData;
                            bat = new TempBAT<v2_h1_select_t, v2_t2_select_t>(
                                    bat_coldesc_head_t(ColumnMetaData(hmd.width, hmd.AN_A, hmd.AN_Ainv, hmd.AN_unencMaxU, hmd.AN_unencMinS)),
                                    bat_coldesc_tail_t(ColumnMetaData(arg2->tail.metaData.width, AT2R, AT2InvR, arg2->tail.metaData.AN_unencMaxU, arg2->tail.metaData.AN_unencMinS)));
                        } else {
                            bat = skeletonJoin<v2_h1_select_t, v2_t2_select_t>(arg1, arg2);
                        }
                        AN_indicator_vector * vec1 = head1_helper_t::createIndicatorVector();
                        AN_indicator_vector * vec2 = tail1_helper_t::createIndicatorVector();
                        AN_indicator_vector * vec3 = head2_helper_t::createIndicatorVector();
                        AN_indicator_vector * vec4 = tail2_helper_t::createIndicatorVector();
                        auto iter1 = arg1->begin();
                        auto iter2 = arg2->begin();
                        if (iter1->hasNext() && iter2->hasNext()) { // only really continue when both BATs are not empty
                            size_t pos = 0;
                            if (hashside == hash_side_t::left) {
                                const larger_t t1max = static_cast<larger_t>(std::numeric_limits<t1unenc_t>::max());
                                google::dense_hash_map<t1unenc_t, std::vector<typename Head1::type_t> > hashMap(arg1->size());
                                hashMap.set_empty_key(Tail1::dhm_emptykey);
                                for (; iter1->hasNext(); ++*iter1, ++pos) { // build
                                    auto h1 = iter1->head();
                                    if (head1_helper_t::isEncoded && static_cast<h1enc_t>(h1 * AH1Inv) > AH1UnencMaxU) {
                                        vec1->push_back(pos * AOID);
                                    }
                                    auto t1 = tail1_helper_t::mulIfEncoded(iter1->tail(), AT1Inv);
                                    if (tail1_helper_t::isEncoded && t1 > AT1UnencMaxU) {
                                        vec2->push_back(pos * AOID);
                                    }
                                    hashMap[static_cast<t1unenc_t>(t1)].push_back(h1);
                                }
                                auto mapEnd = hashMap.end();
                                pos = 0;
                                for (; iter2->hasNext(); ++*iter2, ++pos) { // probe
                                    auto h2 = head2_helper_t::mulIfEncoded(iter2->head(), AH2Inv);
                                    if (head2_helper_t::isEncoded && h2 > AH2UnencMaxU) {
                                        vec3->push_back(pos * AOID);
                                    }
                                    auto t2 = iter2->tail();
                                    if (tail2_helper_t::isEncoded && (t2 * AT2Inv) > AT2UnencMaxU) {
                                        vec4->push_back(pos * AOID);
                                    }
                                    if (static_cast<larger_t>(h2) <= t1max) { // do not probe values which are too large anyways
                                        auto mapIter = hashMap.find(static_cast<t1unenc_t>(h2));
                                        if (mapIter != mapEnd) {
                                            for (auto matched : mapIter->second) {
                                                if constexpr (reencodeHead & reencodeTail) {
                                                    bat->append(std::make_pair(matched * reencFactorH1, t2 * reencFactorT2));
                                                } else if constexpr (reencodeHead) {
                                                    bat->append(std::make_pair(matched * reencFactorH1, t2));
                                                } else if constexpr (reencodeTail) {
                                                    bat->append(std::make_pair(matched, t2 * reencFactorT2));
                                                } else {
                                                    bat->append(std::make_pair(matched, t2));
                                                }
                                            }
                                        }
                                    }
                                }
                            } else {
                                const larger_t h2max = static_cast<larger_t>(std::numeric_limits<h2unenc_t>::max());
                                google::dense_hash_map<h2unenc_t, std::vector<typename Tail2::type_t> > hashMap(arg2->size());
                                hashMap.set_empty_key(Head2::dhm_emptykey);
                                for (; iter2->hasNext(); ++*iter2, ++pos) { // build
                                    auto h2 = head2_helper_t::mulIfEncoded(iter2->head(), AH2Inv);
                                    if (head2_helper_t::isEncoded && h2 > AH2UnencMaxU) {
                                        vec3->push_back(pos * AOID);
                                    }
                                    auto t2 = iter2->tail();
                                    if (tail2_helper_t::isEncoded && static_cast<t2enc_t>(t2 * AT2Inv) > AT2UnencMaxU) {
                                        vec4->push_back(pos * AOID);
                                    }
                                    hashMap[static_cast<h2unenc_t>(h2)].push_back(t2);
                                }
                                auto mapEnd = hashMap.end();
                                pos = 0;
                                for (; iter1->hasNext(); ++*iter1, ++pos) { // probe
                                    auto h1 = iter1->head();
                                    if (head1_helper_t::isEncoded && static_cast<h1enc_t>(h1 * AH1Inv) > AH1UnencMaxU) {
                                        vec1->push_back(pos * AOID);
                                    }
                                    auto t1 = tail1_helper_t::mulIfEncoded(iter1->tail(), AT1Inv);
                                    if (tail1_helper_t::isEncoded && t1 > AT1UnencMaxU) {
                                        vec2->push_back(pos * AOID);
                                    }
                                    if (static_cast<larger_t>(t1) <= h2max) { // do not probe values which are too large anyways
                                        auto mapIter = hashMap.find(static_cast<h2unenc_t>(t1));
                                        if (mapIter != mapEnd) {
                                            for (auto matched : mapIter->second) {
                                                if constexpr (reencodeHead & reencodeTail) {
                                                    bat->append(std::make_pair(h1 * reencFactorH1, matched * reencFactorT2));
                                                } else if constexpr (reencodeHead) {
                                                    bat->append(std::make_pair(h1 * reencFactorH1, matched));
                                                } else if constexpr (reencodeTail) {
                                                    bat->append(std::make_pair(h1, matched * reencFactorT2));
                                                } else {
                                                    bat->append(std::make_pair(h1, matched));
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        delete iter1;
                        delete iter2;

                        return make_tuple(bat, vec1, vec2, vec3, vec4);
                    }
                };

                template<typename Head1, typename Tail1, typename Head2, bool reencode, bool dummy>
                struct hashjoinANunencHashmap<Head1, Tail1, Head2, v2_str_t, reencode, dummy> {

                    typedef typename ReturnTypeSelector<Head1, reencode>::v2_select_t head1_v2_select_t;
                    typedef typename TypeMap<Head1>::v2_encoded_t H1Enc;
                    typedef typename H1Enc::type_t h1enc_t;
                    typedef typename TypeMap<Tail1>::v2_encoded_t T1Enc;
                    typedef typename T1Enc::type_t t1enc_t;
                    typedef typename TypeMap<Head2>::v2_encoded_t H2Enc;
                    typedef typename H2Enc::type_t h2enc_t;
                    typedef typename TypeMap<v2_str_t>::v2_encoded_t T2Enc;
                    typedef typename T2Enc::type_t t2enc_t;
                    typedef typename TypeMap<Tail1>::v2_base_t::type_t t1unenc_t;
                    typedef typename TypeMap<Head2>::v2_base_t::type_t h2unenc_t;
                    typedef typename ahead::larger_type<t1unenc_t, h2unenc_t>::type_t larger_t;
                    typedef ANhelper<Head1> head1_helper_t;
                    typedef ANhelper<Tail1> tail1_helper_t;
                    typedef ANhelper<Head2> head2_helper_t;

                    static std::tuple<TempBAT<head1_v2_select_t, v2_str_t>*, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *> run(
                            BAT<Head1, Tail1>* arg1,
                            BAT<Head2, v2_str_t>* arg2,
                            hash_side_t hashside,
                            resoid_t AOID,
                            h1enc_t AH1R = 1, // for reencode
                            h1enc_t AH1InvR = 1, // for reencode
                            t2enc_t AT2R = nullptr, // dummy
                            t2enc_t AT2InvR = nullptr // dummy
                            ) {
                        (void) AT2R;
                        (void) AT2InvR;
                        const h1enc_t AH1Inv = head1_helper_t::getIfEncoded(arg1->head.metaData.AN_Ainv);
                        const h1enc_t AH1UnencMaxU = arg1->head.metaData.AN_unencMaxU;
                        const t1enc_t AT1Inv = tail1_helper_t::getIfEncoded(arg1->tail.metaData.AN_Ainv);
                        const t1enc_t AT1UnencMaxU = arg1->tail.metaData.AN_unencMaxU;
                        const h2enc_t AH2Inv = head2_helper_t::getIfEncoded(arg2->head.metaData.AN_Ainv);
                        const h2enc_t AH2UnencMaxU = arg2->head.metaData.AN_unencMaxU;
                        const h1enc_t reencFactorH1 = head1_helper_t::mulIfEncoded(AH1R, AH1Inv);
                        TempBAT<head1_v2_select_t, v2_str_t> * bat;
                        if (reencode) {
                            typedef typename TempBAT<head1_v2_select_t, v2_str_t>::coldesc_head_t bat_coldesc_head_t;
                            typedef typename TempBAT<head1_v2_select_t, v2_str_t>::coldesc_tail_t bat_coldesc_tail_t;
                            bat = new TempBAT<head1_v2_select_t, v2_str_t>(
                                    bat_coldesc_head_t(ColumnMetaData(arg1->head.metaData.width, AH1R, AH1InvR, arg1->head.metaData.AN_unencMaxU, arg1->head.metaData.AN_unencMinS)),
                                    bat_coldesc_tail_t(arg2->tail.metaData));
                        } else {
                            bat = skeletonJoin<head1_v2_select_t, v2_str_t>(arg1, arg2);
                        }
                        AN_indicator_vector *vec1 = head1_helper_t::createIndicatorVector();
                        AN_indicator_vector *vec2 = tail1_helper_t::createIndicatorVector();
                        AN_indicator_vector *vec3 = head2_helper_t::createIndicatorVector();
                        auto iter1 = arg1->begin();
                        auto iter2 = arg2->begin();
                        if (iter1->hasNext() && iter2->hasNext()) { // only really continue when both BATs are not empty
                            size_t pos = 0;
                            if (hashside == hash_side_t::left) {
                                const larger_t t1max = static_cast<larger_t>(std::numeric_limits<t1unenc_t>::max());
                                google::dense_hash_map<t1unenc_t, std::vector<typename Head1::type_t> > hashMap(arg1->size());
                                hashMap.set_empty_key(Tail1::dhm_emptykey);
                                for (; iter1->hasNext(); ++*iter1, ++pos) { // build
                                    auto h1 = iter1->head();
                                    auto t1 = iter1->tail();
                                    if (head1_helper_t::isEncoded && (h1 * AH1Inv) > AH1UnencMaxU) {
                                        vec1->push_back(pos * AOID);
                                    }
                                    if (tail1_helper_t::isEncoded && (t1 * AT1Inv) > AT1UnencMaxU) {
                                        vec2->push_back(pos * AOID);
                                    }
                                    if (head2_helper_t::isEncoded) {
                                        hashMap[static_cast<t1unenc_t>(t1 * AT1Inv)].push_back(h1);
                                    } else {
                                        hashMap[t1].push_back(h1);
                                    }
                                }
                                auto mapEnd = hashMap.end();
                                pos = 0;
                                for (; iter2->hasNext(); ++*iter2, ++pos) { // probe
                                    h2enc_t h2 = head2_helper_t::mulIfEncoded(iter2->head(), AH2Inv);
                                    auto t2 = iter2->tail();
                                    if (head2_helper_t::isEncoded && h2 > AH2UnencMaxU) {
                                        vec3->push_back(pos * AOID);
                                    }
                                    if (static_cast<larger_t>(h2) <= t1max) {
                                        auto mapIter = hashMap.find(static_cast<t1unenc_t>(h2));
                                        if (mapIter != mapEnd) {
                                            for (auto matched : mapIter->second) {
                                                if (reencode) {
                                                    bat->append(std::make_pair(matched * reencFactorH1, t2));
                                                } else {
                                                    bat->append(std::make_pair(matched, t2));
                                                }
                                            }
                                        }
                                    }
                                }
                            } else {
                                const larger_t h2max = static_cast<larger_t>(std::numeric_limits<h2unenc_t>::max());
                                google::dense_hash_map<h2unenc_t, std::vector<str_t> > hashMap(arg2->size());
                                hashMap.set_empty_key(Head2::dhm_emptykey);
                                for (; iter2->hasNext(); ++*iter2, ++pos) { // build
                                    h2enc_t h2 = head2_helper_t::mulIfEncoded(iter2->head(), AH2Inv);
                                    auto t2 = iter2->tail();
                                    if (head2_helper_t::isEncoded && static_cast<h2enc_t>(h2) > AH2UnencMaxU) {
                                        vec3->push_back(pos * AOID);
                                    }
                                    if (head2_helper_t::isEncoded) {
                                        hashMap[static_cast<h2unenc_t>(h2)].push_back(t2);
                                    } else {
                                        hashMap[h2].push_back(t2);
                                    }
                                }
                                auto mapEnd = hashMap.end();
                                pos = 0;
                                for (; iter1->hasNext(); ++*iter1, ++pos) { // probe
                                    h1enc_t h1 = head1_helper_t::mulIfEncoded(iter1->head(), AH1Inv);
                                    t1enc_t t1 = tail1_helper_t::mulIfEncoded(iter1->tail(), AT1Inv);
                                    if (head1_helper_t::isEncoded && h1 > AH1UnencMaxU) {
                                        vec1->push_back(pos * AOID);
                                    }
                                    if (tail1_helper_t::isEncoded && t1 > AT1UnencMaxU) {
                                        vec2->push_back(pos * AOID);
                                    }
                                    if (static_cast<larger_t>(t1) <= h2max) {
                                        auto mapIter = hashMap.find(static_cast<h2unenc_t>(t1));
                                        if (mapIter != mapEnd) {
                                            for (auto matched : mapIter->second) {
                                                if (reencode) {
                                                    bat->append(std::make_pair(h1 * reencFactorH1, matched));
                                                } else {
                                                    bat->append(std::make_pair(h1, matched));
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        delete iter1;
                        delete iter2;

                        return make_tuple(bat, vec1, vec2, vec3, nullptr);
                    }
                };

                template<typename Head1, typename Tail1, typename Head2, typename Tail2, bool reencode = false>
                struct hashjoinANencodedHashmap {

                    typedef typename Head1::v2_select_t head1_v2_select_t;
                    typedef typename Tail2::v2_select_t tail2_v2_select_t;
                    typedef typename TypeMap<Head1>::v2_encoded_t H1Enc;
                    typedef typename H1Enc::type_t h1enc_t;
                    typedef typename TypeMap<Tail1>::v2_encoded_t T1Enc;
                    typedef typename T1Enc::type_t t1enc_t;
                    typedef typename TypeMap<Head2>::v2_encoded_t H2Enc;
                    typedef typename H2Enc::type_t h2enc_t;
                    typedef typename TypeMap<Tail2>::v2_encoded_t T2Enc;
                    typedef typename T2Enc::type_t t2enc_t;
                    typedef ahead::larger_type<t1enc_t, h2enc_t> larger_type_t;
                    typedef typename larger_type_t::type_t hash_t;
                    typedef ANhelper<Head1> head1_helper_t;
                    typedef ANhelper<Tail1> tail1_helper_t;
                    typedef ANhelper<Head2> head2_helper_t;
                    typedef ANhelper<Tail2> tail2_helper_t;

                    static std::tuple<TempBAT<head1_v2_select_t, tail2_v2_select_t>*, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *> run(
                            BAT<Head1, Tail1>* arg1,
                            BAT<Head2, Tail2>* arg2,
                            hash_side_t hashside, // by that, by default the order of the left BAT is preserved (what we expect in the queries)
                            resoid_t AOID,
                            h1enc_t AH1R = 1, // for reencode
                            h1enc_t AH1InvR = 1, // for reencode
                            t2enc_t AT2R = 1, // for reencode
                            h1enc_t AT2InvR = 1 // for reencode
                            ) {
                        const h1enc_t AH1Inv = head1_helper_t::getIfEncoded(arg1->head.metaData.AN_Ainv);
                        const h1enc_t AH1UnencMaxU = arg1->head.metaData.AN_unencMaxU;
                        const t1enc_t AT1 = tail1_helper_t::getIfEncoded(arg1->tail.metaData.AN_A);
                        const t1enc_t AT1Inv = tail1_helper_t::getIfEncoded(arg1->tail.metaData.AN_Ainv);
                        const t1enc_t AT1UnencMaxU = arg1->tail.metaData.AN_unencMaxU;
                        const t1enc_t AH2 = head2_helper_t::getIfEncoded(arg2->head.metaData.AN_A);
                        const h2enc_t AH2Inv = head2_helper_t::getIfEncoded(arg2->head.metaData.AN_Ainv);
                        const h2enc_t AH2UnencMaxU = arg2->head.metaData.AN_unencMaxU;
                        const t2enc_t AT2Inv = tail2_helper_t::getIfEncoded(arg2->tail.metaData.AN_Ainv);
                        const t2enc_t AT2UnencMaxU = arg2->tail.metaData.AN_unencMaxU;
                        // do we need any conversion between left Tail and right Head? If so, also regard which of the types is larger
                        const h1enc_t reencFactorH1 = AH1R * AH1Inv;
                        const t2enc_t reencFactorT2 = AT2R * AT2Inv;
                        TempBAT<head1_v2_select_t, tail2_v2_select_t> * bat = nullptr;
                        if (reencode) {
                            typedef typename TempBAT<head1_v2_select_t, tail2_v2_select_t>::coldesc_head_t bat_coldesc_head_t;
                            typedef typename TempBAT<head1_v2_select_t, tail2_v2_select_t>::coldesc_tail_t bat_coldesc_tail_t;
                            bat = new TempBAT<head1_v2_select_t, tail2_v2_select_t>(
                                    bat_coldesc_head_t(ColumnMetaData(arg1->head.metaData.width, AH1R, AH1InvR, arg1->head.metaData.AN_unencMaxU, arg1->head.metaData.AN_unencMinS)),
                                    bat_coldesc_tail_t(ColumnMetaData(arg2->tail.metaData.width, AH1R, AT2InvR, arg2->tail.metaData.AN_unencMaxU, arg2->tail.metaData.AN_unencMinS)));
                        } else {
                            bat = skeletonJoin<head1_v2_select_t, tail2_v2_select_t>(arg1, arg2);
                        }
                        AN_indicator_vector * vec1 = head1_helper_t::createIndicatorVector();
                        AN_indicator_vector * vec2 = tail1_helper_t::createIndicatorVector();
                        AN_indicator_vector * vec3 = head2_helper_t::createIndicatorVector();
                        AN_indicator_vector * vec4 = tail2_helper_t::createIndicatorVector();
                        auto iter1 = arg1->begin();
                        auto iter2 = arg2->begin();
                        if (iter1->hasNext() && iter2->hasNext()) { // only really continue when both BATs are not empty
                            size_t pos = 0;
                            if (hashside == hash_side_t::left) {
                                // next, we search for the inverse we need for matching right head against left tail
                                // if the types are the same, or the right head type is smaller, we can simply use the left tail's inverse
                                // otherwise, the right head type is larger and we must recompute the inverse of AT1 in the larger ring
                                hash_t Ainv;
                                const bool needConvert = (!std::is_same<Tail1, Head2>::value) && (static_cast<hash_t>(AT1) != static_cast<hash_t>(AH2));
                                const constexpr bool isTail1Smaller = sizeof(t1enc_t) < sizeof(h2enc_t);
                                if (needConvert || isTail1Smaller) {
                                    // we need to convert (e.g. different A's or Tail1 is smaller type than Head2 --> recompute the inverse for larger ring)
                                    auto newAinv = ext_euclidean(uint128_t(AT1), sizeof(hash_t) * 8);
                                    Ainv = v2convert<hash_t>(newAinv);
                                } else {
                                    Ainv = AT1Inv;
                                }
                                const hash_t probeFactor = (needConvert || isTail1Smaller) ? (Ainv * static_cast<hash_t>(AH2)) : 1;
                                google::dense_hash_map<hash_t, std::vector<typename Head1::type_t> > hashMap(arg1->size());
                                hashMap.set_empty_key(Tail1::dhm_emptykey);
                                for (; iter1->hasNext(); ++*iter1, ++pos) { // build
                                    auto h1 = iter1->head();
                                    auto t1 = iter1->tail();
                                    if (head1_helper_t::isEncoded && static_cast<h1enc_t>(h1 * AH1Inv) > AH1UnencMaxU) {
                                        vec1->push_back(pos * AOID);
                                    }
                                    if (tail1_helper_t::isEncoded && static_cast<t1enc_t>(t1 * AT1Inv) > AT1UnencMaxU) {
                                        vec2->push_back(pos * AOID);
                                    }
                                    // the above checking for the probe factor requires that we convert here to the A we need for probing
                                    hashMap[static_cast<hash_t>(t1) * probeFactor].push_back(h1);
                                }
                                auto mapEnd = hashMap.end();
                                pos = 0;
                                for (; iter2->hasNext(); ++*iter2, ++pos) { // probe
                                    auto h2 = iter2->head();
                                    auto t2 = iter2->tail();
                                    if (head2_helper_t::isEncoded && static_cast<h2enc_t>(h2 * AH2Inv) > AH2UnencMaxU) {
                                        vec3->push_back(pos * AOID);
                                    }
                                    if (tail2_helper_t::isEncoded && static_cast<t2enc_t>(t2 * AT2Inv) > AT2UnencMaxU) {
                                        vec4->push_back(pos * AOID);
                                    }
                                    auto mapIter = hashMap.find(static_cast<hash_t>(h2));
                                    if (mapIter != mapEnd) {
                                        for (auto matched : mapIter->second) {
                                            if (reencode) {
                                                bat->append(std::make_pair(matched * reencFactorH1, t2 * reencFactorT2));
                                            } else {
                                                bat->append(std::make_pair(matched, t2));
                                            }
                                        }
                                    }
                                }
                            } else {
                                // next, we search for the inverse we need for matching left tail against right head
                                // if the types are the same, or the left tail type is smaller, we can simply use the right head's inverse
                                // otherwise, the left tail's type is larger and we must recompute the inverse of AH2 in the larger ring
                                hash_t Ainv;
                                const bool needConvert = (!std::is_same<Tail1, Head2>::value) && (static_cast<hash_t>(AT1) != static_cast<hash_t>(AH2));
                                const constexpr bool isHead2Smaller = sizeof(h2enc_t) < sizeof(t1enc_t);
                                if (needConvert || isHead2Smaller) {
                                    // we need to convert (e.g. different A's or Tail1 is smaller type than Head2 --> recompute the inverse for larger ring)
                                    auto newAinv = ext_euclidean(uint128_t(AT1), sizeof(hash_t) * 8);
                                    Ainv = v2convert<hash_t>(newAinv);
                                } else {
                                    // TODO Ainv = AH2Inv;
                                    Ainv = AT1Inv;
                                }
                                typedef typename TypeMap<Head2>::v2_base_t::type_t h2unenc_t;
                                const h2unenc_t h2max = std::numeric_limits<h2unenc_t>::max();
                                const hash_t probeFactor = (needConvert || isHead2Smaller) ? (Ainv * static_cast<hash_t>(AH2)) : 1;
                                google::dense_hash_map<h2unenc_t, std::vector<typename Tail2::type_t> > hashMap(arg2->size());
                                hashMap.set_empty_key(Head2::dhm_emptykey);
                                for (; iter2->hasNext(); ++*iter2, ++pos) { // build
                                    auto h = iter2->head();
                                    auto t = iter2->tail();
                                    if (head2_helper_t::isEncoded && static_cast<h2enc_t>(h * AH2Inv) > AH2UnencMaxU) {
                                        vec3->push_back(pos * AOID);
                                    }
                                    if (tail2_helper_t::isEncoded && static_cast<t2enc_t>(t * AT2Inv) > AT2UnencMaxU) {
                                        vec4->push_back(pos * AOID);
                                    }
                                    // the above checking for the probe factor requires that we convert here to the A we need for probing
                                    if (head2_helper_t::isEncoded) {
                                        hashMap[static_cast<h2unenc_t>(h * AH2Inv)].push_back(t);
                                    } else {
                                        hashMap[h].push_back(t);
                                    }
                                }
                                auto mapEnd = hashMap.end();
                                pos = 0;
                                for (; iter1->hasNext(); ++*iter1, ++pos) { // probe
                                    auto h = iter1->head();
                                    auto t = iter1->tail();
                                    if (head1_helper_t::isEncoded && static_cast<h1enc_t>(h * AH1Inv) > AH1UnencMaxU) {
                                        vec1->push_back(pos * AOID);
                                    }
                                    if (tail1_helper_t::isEncoded && static_cast<t1enc_t>(t * AT1Inv) > AT1UnencMaxU) {
                                        vec2->push_back(pos * AOID);
                                    }
                                    h2unenc_t t1 = static_cast<h2unenc_t>(tail1_helper_t::isEncoded ? (t * AT1Inv) : t);
                                    if (t1 <= h2max) {
                                        auto mapIter = hashMap.find(t1);
                                        if (mapIter != mapEnd) {
                                            for (auto matched : mapIter->second) {
                                                if (reencode) {
                                                    bat->append(std::make_pair(h * reencFactorH1, matched * reencFactorT2));
                                                } else {
                                                    bat->append(std::make_pair(h, matched));
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        delete iter1;
                        delete iter2;

                        return make_tuple(bat, vec1, vec2, vec3, vec4);
                    }
                };
            }

            template<typename Head1, typename Tail1, typename Head2, typename Tail2>
            std::tuple<TempBAT<typename Head1::v2_select_t, typename Tail2::v2_select_t>*, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *> hashjoinAN(
                    BAT<Head1, Tail1>* arg1,
                    BAT<Head2, Tail2>* arg2,
                    hash_side_t hashside,
                    resoid_t AOID) {
                return Hashjoin::hashjoinANunencHashmap<Head1, Tail1, Head2, Tail2, false, false>::run(arg1, arg2, hashside, AOID);
            }

            template<typename Head1, typename Tail1, typename Head2, typename Tail2>
            std::tuple<TempBAT<typename TypeMap<Head1>::v2_encoded_t::v2_select_t, typename Tail2::v2_select_t>*, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *,
                    AN_indicator_vector *> hashjoinAN(
                    BAT<Head1, Tail1>* arg1,
                    BAT<Head2, Tail2>* arg2,
                    typename TypeMap<Head1>::v2_encoded_t::type_t AH1Reenc,
                    typename TypeMap<Head1>::v2_encoded_t::type_t AH1InvReenc,
                    hash_side_t hashside,
                    resoid_t AOID) {
                return Hashjoin::hashjoinANunencHashmap<Head1, Tail1, Head2, Tail2, true, false>::run(arg1, arg2, hashside, AOID, AH1Reenc, AH1InvReenc);
            }

            template<typename Head1, typename Tail1, typename Head2, typename Tail2>
            std::tuple<TempBAT<typename TypeMap<Head1>::v2_encoded_t::v2_select_t, typename TypeMap<Tail2>::v2_encoded_t::v2_select_t>*, AN_indicator_vector *, AN_indicator_vector *,
                    AN_indicator_vector *, AN_indicator_vector *> hashjoinAN(
                    BAT<Head1, Tail1>* arg1,
                    BAT<Head2, Tail2>* arg2,
                    typename TypeMap<Head1>::v2_encoded_t::type_t AH1Reenc,
                    typename TypeMap<Head1>::v2_encoded_t::type_t AH1InvReenc,
                    typename TypeMap<Tail2>::v2_encoded_t::type_t AT2Reenc,
                    typename TypeMap<Tail2>::v2_encoded_t::type_t AT2InvReenc,
                    hash_side_t hashside,
                    resoid_t AOID) {
                return Hashjoin::hashjoinANunencHashmap<Head1, Tail1, Head2, Tail2, true, true>::run(arg1, arg2, hashside, AOID, AH1Reenc, AH1InvReenc, AT2Reenc, AT2InvReenc);
            }
        }
    }
}

#endif /* HASHJOIN_AN_TCC */
