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
 * File:   hashjoin.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 22. November 2016, 16:24
 */

#ifndef HASHJOIN_AN_TCC
#define HASHJOIN_AN_TCC

#include <limits>
#include <type_traits>

#include <boost/multiprecision/cpp_int.hpp>

#include <google/dense_hash_map>

#include <ColumnStore.h>

using boost::multiprecision::uint128_t;

namespace v2 {
    namespace bat {
        namespace ops {
            namespace Private {

                template<typename Head1, typename Tail1, typename Head2, typename Tail2, bool reencode = false >
                struct hashjoinANunencHashmap {

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
                    typedef typename TypeMap<Tail1>::v2_base_t::type_t t1unenc_t;
                    typedef typename TypeMap<Head2>::v2_base_t::type_t h2unenc_t;
                    typedef typename v2::larger_type<t1unenc_t, h2unenc_t>::type_t larger_t;

                    tuple<TempBAT<head1_v2_select_t, tail2_v2_select_t>*, vector<bool>*, vector<bool>*, vector<bool>*, vector<bool>*>
                    operator() (
                        BAT<Head1, Tail1>* arg1,
                        BAT<Head2, Tail2>* arg2,
                        hash_side_t hashside = hash_side_t::right, // by that, by default the order of the left BAT is preserved (what we expect in the queries)
                        h1enc_t AH1R = 1, // for reencode
                        h1enc_t AH1InvR = 1, // for reencode
                        t2enc_t AT2R = 1, // for reencode
                        h1enc_t AT2InvR = 1 // for reencode
                        ) {
                        const bool isHead1Encoded = is_base_of<v2_anencoded_t, Head1>::value;
                        const bool isTail1Encoded = is_base_of<v2_anencoded_t, Tail1>::value;
                        const bool isHead2Encoded = is_base_of<v2_anencoded_t, Head2>::value;
                        const bool isTail2Encoded = is_base_of<v2_anencoded_t, Tail2>::value;
                        const h1enc_t AH1Inv = isHead1Encoded ? arg1->head.metaData.AN_Ainv : 1;
                        const h1enc_t AH1UnencMaxU = arg1->head.metaData.AN_unencMaxU;
                        const t1enc_t AT1Inv = isTail1Encoded ? arg1->tail.metaData.AN_Ainv : 1;
                        const t1enc_t AT1UnencMaxU = arg1->tail.metaData.AN_unencMaxU;
                        const h2enc_t AH2Inv = isHead2Encoded ? arg2->head.metaData.AN_Ainv : 1;
                        const h2enc_t AH2UnencMaxU = arg2->head.metaData.AN_unencMaxU;
                        const t2enc_t AT2Inv = isTail2Encoded ? arg2->tail.metaData.AN_Ainv : 1;
                        const t2enc_t AT2UnencMaxU = arg2->tail.metaData.AN_unencMaxU;
                        // do we need any conversion between left Tail and right Head? If so, also regard which of the types is larger
                        const h1enc_t reencFactorH1 = AH1R * AH1Inv;
                        const t2enc_t reencFactorT2 = AT2R * AT2Inv;
                        TempBAT<head1_v2_select_t, tail2_v2_select_t> * bat = nullptr;
                        if (reencode) {
                            typedef typename TempBAT<head1_v2_select_t, tail2_v2_select_t>::coldesc_head_t bat_coldesc_head_t;
                            typedef typename TempBAT<head1_v2_select_t, tail2_v2_select_t>::coldesc_tail_t bat_coldesc_tail_t;
                            bat = new TempBAT<head1_v2_select_t, tail2_v2_select_t>(bat_coldesc_head_t(ColumnMetaData(arg1->head.metaData.width, AH1R, AH1InvR, arg1->head.metaData.AN_unencMaxU, arg1->head.metaData.AN_unencMinS)), bat_coldesc_tail_t(ColumnMetaData(arg2->tail.metaData.width, AH1R, AT2InvR, arg2->tail.metaData.AN_unencMaxU, arg2->tail.metaData.AN_unencMinS)));
                        } else {
                            bat = skeletonJoin<head1_v2_select_t, tail2_v2_select_t>(arg1, arg2);
                        }
                        vector<bool> *vec1 = (isHead1Encoded ? new vector<bool>(arg1->size()) : nullptr);
                        vector<bool> *vec2 = (isTail1Encoded ? new vector<bool>(arg1->size()) : nullptr);
                        vector<bool> *vec3 = (isHead2Encoded ? new vector<bool>(arg2->size()) : nullptr);
                        vector<bool> *vec4 = (isTail2Encoded ? new vector<bool>(arg2->size()) : nullptr);
                        auto iter1 = arg1->begin();
                        auto iter2 = arg2->begin();
                        // std::cout << "head1 [" << arg1->head.metaData.width << ", " << arg1->head.metaData.seqbase << ", " << (arg1->head.metaData.isEncoded ? "enc" : "unenc") << ", " << arg1->head.metaData.AN_A << ", " << AH1Inv << ", " << static_cast<typename Head1::unenc_v2_t::type_t>(arg1->head.metaData.AN_A * AH1Inv) << ", " << arg1->head.metaData.AN_unencMaxU << ", " << arg1->head.metaData.AN_unencMinS << "]\ntail1 [" << arg1->tail.metaData.width << ", " << arg1->tail.metaData.seqbase << ", " << (arg1->tail.metaData.isEncoded ? "enc" : "unenc") << ", " << arg1->tail.metaData.AN_A << ", " << AT1Inv << ", " << static_cast<typename Tail1::unenc_v2_t::type_t>(arg1->tail.metaData.AN_A * AT1Inv) << ", " << arg1->tail.metaData.AN_unencMaxU << ", " << arg1->tail.metaData.AN_unencMinS << "]\nhead2 [" << arg2->head.metaData.width << ", " << arg2->head.metaData.seqbase << ", " << (arg2->head.metaData.isEncoded ? "enc" : "unenc") << ", " << arg2->head.metaData.AN_A << ", " << AH2Inv << ", " << static_cast<typename Head2::unenc_v2_t::type_t>(arg2->head.metaData.AN_A * AH2Inv) << ", " << arg2->head.metaData.AN_unencMaxU << ", " << arg2->head.metaData.AN_unencMinS << "]\ntail2 [" << arg2->tail.metaData.width << ", " << arg2->tail.metaData.seqbase << ", " << (arg2->tail.metaData.isEncoded ? "enc" : "unenc") << ", " << arg2->tail.metaData.AN_A << ", " << AT2Inv << ", " << static_cast<typename Tail2::unenc_v2_t::type_t>(arg2->tail.metaData.AN_A * AT2Inv) << ", " << arg2->tail.metaData.AN_unencMaxU << ", " << arg2->tail.metaData.AN_unencMinS << "]\n";
                        if (iter1->hasNext() && iter2->hasNext()) { // only really continue when both BATs are not empty
                            size_t pos = 0;
                            if (hashside == hash_side_t::left) {
                                const larger_t t1max = static_cast<larger_t>(std::numeric_limits<t1unenc_t>::max());
                                google::dense_hash_map<t1unenc_t, vector<typename Head1::type_t> > hashMap(arg1->size());
                                hashMap.set_empty_key(Tail1::dhm_emptykey);
                                for (; iter1->hasNext(); ++*iter1, ++pos) { // build
                                    auto h1 = iter1->head();
                                    if (isHead1Encoded && static_cast<h1enc_t>(h1 * AH1Inv) > AH1UnencMaxU) {
                                        (*vec1)[pos] = true;
                                    }
                                    auto t1 = isTail1Encoded ? static_cast<t1enc_t>(iter1->tail() * AT1Inv) : iter1->tail();
                                    if (isTail1Encoded && t1 > AT1UnencMaxU) {
                                        (*vec2)[pos] = true;
                                    }
                                    if (isHead2Encoded) {
                                        hashMap[static_cast<t1unenc_t>(t1)].push_back(h1);
                                    } else {
                                        hashMap[t1].push_back(h1);
                                    }
                                }
                                auto mapEnd = hashMap.end();
                                pos = 0;
                                for (; iter2->hasNext(); ++*iter2, ++pos) { // probe
                                    auto h2 = isHead2Encoded ? static_cast<h2enc_t>(iter2->head() * AH2Inv) : iter2->head();
                                    if (isHead2Encoded && h2 > AH2UnencMaxU) {
                                        (*vec3)[pos] = true;
                                    }
                                    auto t = iter2->tail();
                                    if (isTail2Encoded && (t * AT2Inv) > AT2UnencMaxU) {
                                        (*vec4)[pos] = true;
                                    }
                                    auto h2l = static_cast<larger_t>(h2);
                                    if (h2l <= t1max) {
                                        auto mapIter = hashMap.find(static_cast<t1unenc_t>(h2l));
                                        if (mapIter != mapEnd) {
                                            for (auto matched : mapIter->second) {
                                                if (reencode) {
                                                    bat->append(std::make_pair(matched * reencFactorH1, t * reencFactorT2));
                                                } else {
                                                    bat->append(std::make_pair(matched, t));
                                                }
                                            }
                                        }
                                    }
                                }
                            } else {
                                const larger_t h2max = static_cast<larger_t>(std::numeric_limits<h2unenc_t>::max());
                                google::dense_hash_map<h2unenc_t, vector<typename Tail2::type_t> > hashMap(arg2->size());
                                hashMap.set_empty_key(Head2::dhm_emptykey);
                                for (; iter2->hasNext(); ++*iter2, ++pos) { // build
                                    auto h2 = isHead2Encoded ? static_cast<h2enc_t>(iter2->head() * AH2Inv) : iter2->head();
                                    if (isHead2Encoded && h2 > AH2UnencMaxU) {
                                        (*vec3)[pos] = true;
                                    }
                                    auto t2 = iter2->tail();
                                    if (isTail2Encoded && static_cast<t2enc_t>(t2 * AT2Inv) > AT2UnencMaxU) {
                                        (*vec4)[pos] = true;
                                    }
                                    hashMap[static_cast<h2unenc_t>(h2)].push_back(t2);
                                }
                                auto mapEnd = hashMap.end();
                                pos = 0;
                                for (; iter1->hasNext(); ++*iter1, ++pos) { // probe
                                    auto h1 = iter1->head();
                                    if (isHead1Encoded && static_cast<h1enc_t>(h1 * AH1Inv) > AH1UnencMaxU) {
                                        (*vec1)[pos] = true;
                                    }
                                    auto t = isTail1Encoded ? static_cast<t1enc_t>(iter1->tail() * AT1Inv) : iter1->tail();
                                    if (isTail1Encoded && t > AT1UnencMaxU) {
                                        (*vec2)[pos] = true;
                                    }
                                    if (static_cast<larger_t>(t) <= h2max) {
                                        auto mapIter = hashMap.find(static_cast<h2unenc_t>(t));
                                        if (mapIter != mapEnd) {
                                            for (auto matched : mapIter->second) {
                                                if (reencode) {
                                                    bat->append(std::make_pair(h1 * reencFactorH1, matched * reencFactorT2));
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

                template<typename Head1, typename Tail1, typename Head2, bool reencode>
                struct hashjoinANunencHashmap<Head1, Tail1, Head2, v2_str_t, reencode> {

                    typedef typename Head1::v2_select_t head1_v2_select_t;
                    typedef typename TypeMap<Head1>::v2_encoded_t H1Enc;
                    typedef typename H1Enc::type_t h1enc_t;
                    typedef typename TypeMap<Tail1>::v2_encoded_t T1Enc;
                    typedef typename T1Enc::type_t t1enc_t;
                    typedef typename TypeMap<Head2>::v2_encoded_t H2Enc;
                    typedef typename H2Enc::type_t h2enc_t;
                    typedef typename TypeMap<Tail1>::v2_base_t::type_t t1unenc_t;
                    typedef typename TypeMap<Head2>::v2_base_t::type_t h2unenc_t;
                    typedef typename v2::larger_type<t1unenc_t, h2unenc_t>::type_t larger_t;

                    tuple<TempBAT<head1_v2_select_t, v2_str_t>*, vector<bool>*, vector<bool>*, vector<bool>*, vector<bool>*>
                    operator() (
                        BAT<Head1, Tail1>* arg1,
                        BAT<Head2, v2_str_t>* arg2,
                        hash_side_t hashside = hash_side_t::right,
                        h1enc_t AH1r = 1, // for reencode
                        h1enc_t AH1InvR = 1 // for reencode
                        ) {
                        const bool isHead1Encoded = is_base_of<v2_anencoded_t, Head1>::value;
                        const bool isTail1Encoded = is_base_of<v2_anencoded_t, Tail1>::value;
                        const bool isHead2Encoded = is_base_of<v2_anencoded_t, Head2>::value;
                        const h1enc_t AH1Inv = arg1->head.metaData.AN_Ainv;
                        const h1enc_t AH1UnencMaxU = arg1->head.metaData.AN_unencMaxU;
                        const t1enc_t AT1Inv = arg1->tail.metaData.AN_Ainv;
                        const t1enc_t AT1UnencMaxU = arg1->tail.metaData.AN_unencMaxU;
                        const h2enc_t AH2Inv = arg2->head.metaData.AN_Ainv;
                        const h2enc_t AH2UnencMaxU = arg2->head.metaData.AN_unencMaxU;
                        const h1enc_t reencFactorH1 = AH1r * AH1Inv;
                        TempBAT<head1_v2_select_t, v2_str_t> * bat;
                        if (reencode) {
                            typedef typename TempBAT<head1_v2_select_t, v2_str_t>::coldesc_head_t bat_coldesc_head_t;
                            typedef typename TempBAT<head1_v2_select_t, v2_str_t>::coldesc_tail_t bat_coldesc_tail_t;
                            bat = new TempBAT<head1_v2_select_t, v2_str_t>(bat_coldesc_head_t(ColumnMetaData(arg1->head.metaData.width, AH1r, AH1InvR, arg1->head.metaData.AN_unencMaxU, arg1->head.metaData.AN_unencMinS)), bat_coldesc_tail_t(arg2->tail.metaData));
                        } else {
                            bat = skeletonJoin<head1_v2_select_t, v2_str_t>(arg1, arg2);
                        }
                        vector<bool> *vec1 = (isHead1Encoded ? new vector<bool>(arg1->size()) : nullptr);
                        vector<bool> *vec2 = (isTail1Encoded ? new vector<bool>(arg1->size()) : nullptr);
                        vector<bool> *vec3 = (isHead2Encoded ? new vector<bool>(arg2->size()) : nullptr);
                        auto iter1 = arg1->begin();
                        auto iter2 = arg2->begin();
                        if (iter1->hasNext() && iter2->hasNext()) { // only really continue when both BATs are not empty
                            size_t pos = 0;
                            if (hashside == hash_side_t::left) {
                                const larger_t t1max = static_cast<larger_t>(std::numeric_limits<t1unenc_t>::max());
                                google::dense_hash_map<t1unenc_t, vector<typename Head1::type_t> > hashMap(arg1->size());
                                hashMap.set_empty_key(Tail1::dhm_emptykey);
                                for (; iter1->hasNext(); ++*iter1, ++pos) { // build
                                    auto h1 = iter1->head();
                                    auto t1 = iter1->tail();
                                    if (isHead1Encoded && (h1 * AH1Inv) > AH1UnencMaxU) {
                                        (*vec1)[pos] = true;
                                    }
                                    if (isTail1Encoded && (t1 * AT1Inv) > AT1UnencMaxU) {
                                        (*vec2)[pos] = true;
                                    }
                                    if (isHead2Encoded) {
                                        hashMap[static_cast<t1unenc_t>(t1 * AT1Inv)].push_back(h1);
                                    } else {
                                        hashMap[t1].push_back(h1);
                                    }
                                }
                                auto mapEnd = hashMap.end();
                                pos = 0;
                                for (; iter2->hasNext(); ++*iter2, ++pos) { // probe
                                    auto h = iter2->head();
                                    auto t = iter2->tail();
                                    if (isHead2Encoded && (h * AH2Inv) > AH2UnencMaxU) {
                                        (*vec3)[pos] = true;
                                    }
                                    auto h2 = static_cast<larger_t>(isHead2Encoded ? (h * AH2Inv) : h);
                                    if (h2 <= t1max) {
                                        auto mapIter = hashMap.find(static_cast<t1unenc_t>(h2));
                                        if (mapIter != mapEnd) {
                                            for (auto matched : mapIter->second) {
                                                if (reencode) {
                                                    bat->append(std::make_pair(matched * reencFactorH1, t));
                                                } else {
                                                    bat->append(std::make_pair(matched, t));
                                                }
                                            }
                                        }
                                    }
                                }
                            } else {
                                const larger_t h2max = static_cast<larger_t>(std::numeric_limits<h2unenc_t>::max());
                                google::dense_hash_map<h2unenc_t, vector<str_t> > hashMap(arg2->size());
                                hashMap.set_empty_key(Head2::dhm_emptykey);
                                for (; iter2->hasNext(); ++*iter2, ++pos) { // build
                                    auto h = iter2->head();
                                    auto t = iter2->tail();
                                    if (isHead2Encoded && (h * AH2Inv) > AH2UnencMaxU) {
                                        (*vec3)[pos] = true;
                                    }
                                    if (isHead2Encoded) {
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
                                    if (isHead1Encoded && (h * AH1Inv) > AH1UnencMaxU) {
                                        (*vec1)[pos] = true;
                                    }
                                    if (isTail1Encoded && (t * AT1Inv) > AT1UnencMaxU) {
                                        (*vec2)[pos] = true;
                                    }
                                    auto t1 = static_cast<larger_t>(isTail1Encoded ? (t * AT1Inv) : t);
                                    if (t1 <= h2max) {
                                        auto mapIter = hashMap.find(static_cast<h2unenc_t>(t1));
                                        if (mapIter != mapEnd) {
                                            for (auto matched : mapIter->second) {
                                                if (reencode) {
                                                    bat->append(std::make_pair(h * reencFactorH1, matched));
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

                        return make_tuple(bat, vec1, vec2, vec3, nullptr);
                    }
                };

                template<typename Head1, typename Tail1, typename Head2, typename Tail2, bool reencode = false >
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
                    typedef v2::larger_type<t1enc_t, h2enc_t> larger_type_t;
                    typedef typename larger_type_t::type_t hash_t;

                    tuple<TempBAT<head1_v2_select_t, tail2_v2_select_t>*, vector<bool>*, vector<bool>*, vector<bool>*, vector<bool>*>
                    operator() (
                        BAT<Head1, Tail1>* arg1,
                        BAT<Head2, Tail2>* arg2,
                        hash_side_t hashside = hash_side_t::right, // by that, by default the order of the left BAT is preserved (what we expect in the queries)
                        h1enc_t AH1R = 1, // for reencode
                        h1enc_t AH1InvR = 1, // for reencode
                        t2enc_t AT2R = 1, // for reencode
                        h1enc_t AT2InvR = 1 // for reencode
                        ) {
                        const bool isHead1Encoded = is_base_of<v2_anencoded_t, Head1>::value;
                        const bool isTail1Encoded = is_base_of<v2_anencoded_t, Tail1>::value;
                        const bool isHead2Encoded = is_base_of<v2_anencoded_t, Head2>::value;
                        const bool isTail2Encoded = is_base_of<v2_anencoded_t, Tail2>::value;
                        const h1enc_t AH1Inv = isHead1Encoded ? arg1->head.metaData.AN_A : 1;
                        const h1enc_t AH1UnencMaxU = arg1->head.metaData.AN_unencMaxU;
                        const t1enc_t AT1 = isTail1Encoded ? arg1->tail.metaData.AN_A : 1;
                        const t1enc_t AT1Inv = isTail1Encoded ? arg1->tail.metaData.AN_Ainv : 1;
                        const t1enc_t AT1UnencMaxU = arg1->tail.metaData.AN_unencMaxU;
                        const h2enc_t AH2 = isHead2Encoded ? arg2->head.metaData.AN_A : 1;
                        const h2enc_t AH2Inv = isHead2Encoded ? arg2->head.metaData.AN_A : 1;
                        const h2enc_t AH2UnencMaxU = arg2->head.metaData.AN_unencMaxU;
                        const t2enc_t AT2Inv = isTail2Encoded ? arg2->tail.metaData.AN_Ainv : 1;
                        const t2enc_t AT2UnencMaxU = arg2->tail.metaData.AN_unencMaxU;
                        // do we need any conversion between left Tail and right Head? If so, also regard which of the types is larger
                        const h1enc_t reencFactorH1 = AH1R * AH1Inv;
                        const t2enc_t reencFactorT2 = AT2R * AT2Inv;
                        TempBAT<head1_v2_select_t, tail2_v2_select_t> * bat = nullptr;
                        if (reencode) {
                            typedef typename TempBAT<head1_v2_select_t, tail2_v2_select_t>::coldesc_head_t bat_coldesc_head_t;
                            typedef typename TempBAT<head1_v2_select_t, tail2_v2_select_t>::coldesc_tail_t bat_coldesc_tail_t;
                            bat = new TempBAT<head1_v2_select_t, tail2_v2_select_t>(bat_coldesc_head_t(ColumnMetaData(arg1->head.metaData.width, AH1R, AH1InvR, arg1->head.metaData.AN_unencMaxU, arg1->head.metaData.AN_unencMinS)), bat_coldesc_tail_t(ColumnMetaData(arg2->tail.metaData.width, AH1R, AT2InvR, arg2->tail.metaData.AN_unencMaxU, arg2->tail.metaData.AN_unencMinS)));
                        } else {
                            bat = skeletonJoin<head1_v2_select_t, tail2_v2_select_t>(arg1, arg2);
                        }
                        vector<bool> *vec1 = (isHead1Encoded ? new vector<bool>(arg1->size()) : nullptr);
                        vector<bool> *vec2 = (isTail1Encoded ? new vector<bool>(arg1->size()) : nullptr);
                        vector<bool> *vec3 = (isHead2Encoded ? new vector<bool>(arg2->size()) : nullptr);
                        vector<bool> *vec4 = (isTail2Encoded ? new vector<bool>(arg2->size()) : nullptr);
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
                                constexpr const bool isTail1Smaller = sizeof (t1enc_t) < sizeof (h2enc_t);
                                if (needConvert || isTail1Smaller) {
                                    // we need to convert (e.g. different A's or Tail1 is smaller type than Head2 --> recompute the inverse for larger ring)
                                    auto newAinv = ext_euclidean(uint128_t(AT1), sizeof (hash_t) * 8);
                                    Ainv = 0;
                                    // OK, let's convert it from that super long representation into our shorter one
                                    const unsigned limb_num = newAinv.backend().size(); // number of limbs
                                    const unsigned limb_bits = sizeof (boost::multiprecision::limb_type) * CHAR_BIT; // size of limb in bits
                                    for (unsigned i = 0; i < limb_num && ((i * limb_bits) < (sizeof (Ainv) * 8)); ++i) {
                                        Ainv |= (newAinv.backend().limbs()[i]) << (i * limb_bits);
                                    }
                                } else {
                                    Ainv = AT1Inv;
                                }
                                const hash_t probeFactor = (needConvert || isTail1Smaller) ? (Ainv * static_cast<hash_t>(AH2)) : 1;
                                google::dense_hash_map<hash_t, vector<typename Head1::type_t> > hashMap(arg1->size());
                                hashMap.set_empty_key(Tail1::dhm_emptykey);
                                for (; iter1->hasNext(); ++*iter1, ++pos) { // build
                                    auto h1 = iter1->head();
                                    auto t1 = iter1->tail();
                                    if (isHead1Encoded && (h1 * AH1Inv) > AH1UnencMaxU) {
                                        (*vec1)[pos] = true;
                                    }
                                    if (isTail1Encoded && (t1 * AT1Inv) > AT1UnencMaxU) {
                                        (*vec2)[pos] = true;
                                    }
                                    // the above checking for the probe factor requires that we convert here to the A we need for probing
                                    hashMap[static_cast<hash_t>(t1) * probeFactor].push_back(h1);
                                }
                                auto mapEnd = hashMap.end();
                                pos = 0;
                                for (; iter2->hasNext(); ++*iter2, ++pos) { // probe
                                    auto h2 = iter2->head();
                                    auto t2 = iter2->tail();
                                    if (isHead2Encoded && (h2 * AH2Inv) > AH2UnencMaxU) {
                                        (*vec3)[pos] = true;
                                    }
                                    if (isTail2Encoded && (t2 * AT2Inv) > AT2UnencMaxU) {
                                        (*vec4)[pos] = true;
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
                                constexpr const bool isHead2Smaller = sizeof (h2enc_t) < sizeof (t1enc_t);
                                if (needConvert || isHead2Smaller) {
                                    // we need to convert (e.g. different A's or Tail1 is smaller type than Head2 --> recompute the inverse for larger ring)
                                    // TODO auto newAinv = ext_euclidean(uint128_t(AH2), sizeof (hash_t) * 8);
                                    auto newAinv = ext_euclidean(uint128_t(AT1), sizeof (h2enc_t) * 8);
                                    Ainv = 0;
                                    // OK, let's convert it from that super long representation into our shorter one
                                    const unsigned limb_num = newAinv.backend().size(); // number of limbs
                                    const unsigned limb_bits = sizeof (boost::multiprecision::limb_type) * CHAR_BIT; // size of limb in bits
                                    for (unsigned i = 0; i < limb_num && ((i * limb_bits) < (sizeof (Ainv) * 8)); ++i) {
                                        Ainv |= (newAinv.backend().limbs()[i]) << (i * limb_bits);
                                    }
                                } else {
                                    // TODO Ainv = AH2Inv;
                                    Ainv = AT1Inv;
                                }
                                typedef typename TypeMap<Head2>::v2_base_t::type_t h2unenc_t;
                                const h2unenc_t h2max = std::numeric_limits<h2unenc_t>::max();
                                // TODO const hash_t probeFactor = (needConvert || isHead2Smaller) ? (Ainv * static_cast<hash_t>(AT1)) : 1;
                                const hash_t probeFactor = (needConvert || isHead2Smaller) ? (Ainv * static_cast<hash_t>(AH2)) : 1;
                                google::dense_hash_map<h2unenc_t, vector<typename Tail2::type_t> > hashMap(arg2->size());
                                hashMap.set_empty_key(Head2::dhm_emptykey);
                                for (; iter2->hasNext(); ++*iter2, ++pos) { // build
                                    auto h = iter2->head();
                                    auto t = iter2->tail();
                                    if (isHead2Encoded && (h * AH2Inv) > AH2UnencMaxU) {
                                        (*vec3)[pos] = true;
                                    }
                                    if (isTail2Encoded && (t * AT2Inv) > AT2UnencMaxU) {
                                        (*vec4)[pos] = true;
                                    }
                                    // the above checking for the probe factor requires that we convert here to the A we need for probing
                                    if (isHead2Encoded) {
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
                                    if (isHead1Encoded && (h * AH1Inv) > AH1UnencMaxU) {
                                        (*vec1)[pos] = true;
                                    }
                                    if (isTail1Encoded && (t * AT1Inv) > AT1UnencMaxU) {
                                        (*vec2)[pos] = true;
                                    }
                                    h2unenc_t t1 = static_cast<h2unenc_t>(isTail1Encoded ? (t * AT1Inv) : t);
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
            tuple<TempBAT<typename Head1::v2_select_t, typename Tail2::v2_select_t>*, vector<bool>*, vector<bool>*, vector<bool>*, vector<bool>*>
            hashjoinAN (
                        BAT<Head1, Tail1>* arg1,
                        BAT<Head2, Tail2>* arg2,
                        hash_side_t hashside = hash_side_t::right
                        ) {

                return Private::hashjoinANunencHashmap<Head1, Tail1, Head2, Tail2, false>()(arg1, arg2, hashside);
            }

            template<typename Head1, typename Tail1, typename Head2, typename Tail2>
            tuple<TempBAT<typename Head1::v2_select_t, typename Tail2::v2_select_t>*, vector<bool>*, vector<bool>*, vector<bool>*, vector<bool>*>
            hashjoinAN (
                        BAT<Head1, Tail1>* arg1,
                        BAT<Head2, Tail2>* arg2,
                        typename TypeMap<Tail2>::v2_encoded_t::type_t AH1reenc,
                        typename TypeMap<Head1>::v2_encoded_t::type_t AH1InvReenc,
                        hash_side_t hashside = hash_side_t::right
                        ) {

                return Private::hashjoinANunencHashmap<Head1, Tail1, Head2, Tail2, true>()(arg1, arg2, hashside, AH1reenc, AH1InvReenc);
            }

            template<typename Head1, typename Tail1, typename Head2, typename Tail2>
            tuple<TempBAT<typename Head1::v2_select_t, typename Tail2::v2_select_t>*, vector<bool>*, vector<bool>*, vector<bool>*, vector<bool>*>
            hashjoinAN (
                        BAT<Head1, Tail1>* arg1,
                        BAT<Head2, Tail2>* arg2,
                        typename TypeMap<Head1>::v2_encoded_t::type_t AH1Reenc,
                        typename TypeMap<Head1>::v2_encoded_t::type_t AH1InvReenc,
                        typename TypeMap<Tail2>::v2_encoded_t::type_t AT2Reenc,
                        typename TypeMap<Tail2>::v2_encoded_t::type_t AT2InvReenc,
                        hash_side_t hashside = hash_side_t::right
                        ) {
                return Private::hashjoinANunencHashmap<Head1, Tail1, Head2, Tail2, true>()(arg1, arg2, hashside, AH1Reenc, AH1InvReenc, AT2Reenc, AT2InvReenc);
            }
        }
    }
}

#endif /* HASHJOIN_AN_TCC */
