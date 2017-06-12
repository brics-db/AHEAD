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
 * File:   matchjoin.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 2. Januar 2017, 22:38
 */

#ifndef MATCHJOIN_AN_TCC
#define MATCHJOIN_AN_TCC

#include <limits>
#include <type_traits>

#include <boost/multiprecision/cpp_int.hpp>

#include <google/dense_hash_map>

#include <column_storage/Storage.hpp>
#include <column_operators/ANbase.hpp>
#include "../miscellaneous.hpp"
#include <util/v2typeconversion.hpp>

using boost::multiprecision::uint128_t;

namespace ahead {
    namespace bat {
        namespace ops {

            namespace Private {

                template<typename T, bool>
                struct ReturnTypeSelector {

                    typedef typename T::v2_select_t v2_select_t;
                };

                template<typename T>
                struct ReturnTypeSelector<T, true> {

                    typedef typename TypeMap<T>::v2_encoded_t::v2_select_t v2_select_t;
                };

                template<typename Head1, typename Tail1, typename Head2, typename Tail2, bool reencode>
                struct MatchjoinAN {

                    typedef typename Head1::type_t head1_t;
                    typedef typename Tail1::type_t tail1_t;
                    typedef typename Head2::type_t head2_t;
                    typedef typename Tail2::type_t tail2_t;
                    typedef typename ReturnTypeSelector<Head1, reencode>::v2_select_t v2_h1_select_t;
                    typedef typename ReturnTypeSelector<Tail2, reencode>::v2_select_t v2_t2_select_t;
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

                    static std::tuple<BAT<v2_h1_select_t, v2_t2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> run(
                            BAT<Head1, Tail1>* arg1,
                            BAT<Head2, Tail2>* arg2,
                            h1enc_t AH1R = 1, // for reencode
                            h1enc_t AH1InvR = 1, // for reencode
                            t2enc_t AT2R = 1, // for reencode
                            t2enc_t AT2InvR = 1 // for reencode
                            ) {
                        constexpr const bool isHead1Encoded = std::is_base_of<v2_anencoded_t, Head1>::value;
                        constexpr const bool isTail1Encoded = std::is_base_of<v2_anencoded_t, Tail1>::value;
                        constexpr const bool isHead2Encoded = std::is_base_of<v2_anencoded_t, Head2>::value;
                        constexpr const bool isTail2Encoded = std::is_base_of<v2_anencoded_t, Tail2>::value;
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
                        TempBAT<v2_h1_select_t, v2_t2_select_t> * bat = nullptr;
                        if (reencode) {
                            typedef typename TempBAT<v2_h1_select_t, v2_t2_select_t>::coldesc_head_t bat_coldesc_head_t;
                            typedef typename TempBAT<v2_h1_select_t, v2_t2_select_t>::coldesc_tail_t bat_coldesc_tail_t;
                            bat = new TempBAT<v2_h1_select_t, v2_t2_select_t>(
                                    bat_coldesc_head_t(ColumnMetaData(arg1->head.metaData.width, AH1R, AH1InvR, arg1->head.metaData.AN_unencMaxU, arg1->head.metaData.AN_unencMinS)),
                                    bat_coldesc_tail_t(ColumnMetaData(arg2->tail.metaData.width, AT2R, AT2InvR, arg2->tail.metaData.AN_unencMaxU, arg2->tail.metaData.AN_unencMinS)));
                        } else {
                            bat = skeletonJoin<v2_h1_select_t, v2_t2_select_t>(arg1, arg2);
                        }
                        std::vector<bool> *vec1 = (isHead1Encoded ? new std::vector<bool>(arg1->size()) : nullptr);
                        std::vector<bool> *vec2 = (isTail1Encoded ? new std::vector<bool>(arg1->size()) : nullptr);
                        std::vector<bool> *vec3 = (isHead2Encoded ? new std::vector<bool>(arg2->size()) : nullptr);
                        std::vector<bool> *vec4 = (isTail2Encoded ? new std::vector<bool>(arg2->size()) : nullptr);
                        auto iter1 = arg1->begin();
                        auto iter2 = arg2->begin();
                        size_t pos1 = 0;
                        size_t pos2 = 0;
                        while (iter1->hasNext() && iter2->hasNext()) {
                            h2enc_t h2 = isHead2Encoded ? static_cast<h2enc_t>(iter2->head() * AH2Inv) : iter2->head();
                            if (isHead2Encoded && (h2 > AH2UnencMaxU)) {
                                (*vec3)[pos2] = true;
                            }
                            if (isTail2Encoded && ((iter2->tail() * AT2Inv) > AT2UnencMaxU)) {
                                (*vec4)[pos2] = true;
                            }
                            t1enc_t t1 = isTail1Encoded ? static_cast<t1enc_t>(iter1->tail() * AT1Inv) : iter1->tail();
                            for (; iter1->hasNext() && t1 < h2; ++*iter1, ++pos1, t1 = isTail1Encoded ? (iter1->tail() * AT1Inv) : iter1->tail()) {
                                if (isHead1Encoded && (static_cast<h1enc_t>(iter1->head() * AH1Inv) > AH1UnencMaxU)) {
                                    (*vec1)[pos1] = true;
                                }
                                if (isTail1Encoded && (t1 > AT1UnencMaxU)) {
                                    (*vec2)[pos1] = true;
                                }
                            }
                            for (; iter2->hasNext() && t1 > h2; ++*iter2, ++pos2, h2 = isHead2Encoded ? (iter2->head() * AH2Inv) : iter2->head()) {
                                if (isHead2Encoded && (h2 > AH2UnencMaxU)) {
                                    (*vec3)[pos2] = true;
                                }
                                if (isTail2Encoded && (static_cast<t2enc_t>(iter2->tail() * AT2Inv) > AT2UnencMaxU)) {
                                    (*vec4)[pos2] = true;
                                }
                            }
                            if (t1 == h2) {
                                if (reencode) {
                                    bat->append(std::make_pair(iter1->head() * reencFactorH1, iter2->tail() * reencFactorT2));
                                } else {
                                    bat->append(std::make_pair(iter1->head(), iter2->tail()));
                                }
                                ++*iter1;
                                ++pos1;
                                ++*iter2;
                                ++pos2;
                            }
                        }

                        delete iter1;
                        delete iter2;
                        return make_tuple(bat, vec1, vec2, vec3, vec4);
                    }
                };

                template<typename Head1, typename Tail1, typename Head2, bool reencode>
                struct MatchjoinAN<Head1, Tail1, Head2, v2_str_t, reencode> {

                    typedef typename Head1::type_t head1_t;
                    typedef typename Tail1::type_t tail1_t;
                    typedef typename Head2::type_t head2_t;
                    typedef typename v2_str_t::type_t tail2_t;
                    typedef typename ReturnTypeSelector<Head1, reencode>::v2_select_t v2_h1_select_t;
                    typedef typename ReturnTypeSelector<v2_str_t, reencode>::v2_select_t v2_t2_select_t;
                    typedef typename TypeMap<Head1>::v2_encoded_t H1Enc;
                    typedef typename H1Enc::type_t h1enc_t;
                    typedef typename TypeMap<Tail1>::v2_encoded_t T1Enc;
                    typedef typename T1Enc::type_t t1enc_t;
                    typedef typename TypeMap<Head2>::v2_encoded_t H2Enc;
                    typedef typename H2Enc::type_t h2enc_t;
                    typedef typename TypeMap<Head1>::v2_base_t::type_t h1unenc_t;
                    typedef typename TypeMap<Tail1>::v2_base_t::type_t t1unenc_t;
                    typedef typename TypeMap<Head2>::v2_base_t::type_t h2unenc_t;
                    typedef typename ahead::larger_type<t1unenc_t, h2unenc_t>::type_t larger_t;

                    static std::tuple<BAT<v2_h1_select_t, v2_t2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> run(
                            BAT<Head1, Tail1>* arg1,
                            BAT<Head2, v2_str_t>* arg2,
                            h1enc_t AH1R = 1, // for reencode
                            h1enc_t AH1InvR = 1, // for reencode
                            __attribute__((unused))  str_t dummy1 = nullptr,
                            __attribute__((unused))  str_t dummy2 = nullptr) {
                        constexpr const bool isHead1Encoded = std::is_base_of<v2_anencoded_t, Head1>::value;
                        constexpr const bool isTail1Encoded = std::is_base_of<v2_anencoded_t, Tail1>::value;
                        constexpr const bool isHead2Encoded = std::is_base_of<v2_anencoded_t, Head2>::value;
                        const h1enc_t AH1Inv = isHead1Encoded ? arg1->head.metaData.AN_Ainv : 1;
                        const h1enc_t AH1UnencMaxU = arg1->head.metaData.AN_unencMaxU;
                        const t1enc_t AT1Inv = isTail1Encoded ? arg1->tail.metaData.AN_Ainv : 1;
                        const t1enc_t AT1UnencMaxU = arg1->tail.metaData.AN_unencMaxU;
                        const h2enc_t AH2Inv = isHead2Encoded ? arg2->head.metaData.AN_Ainv : 1;
                        const h2enc_t AH2UnencMaxU = arg2->head.metaData.AN_unencMaxU;
                        // do we need any conversion between left Tail and right Head? If so, also regard which of the types is larger
                        const h1enc_t reencFactorH1 = AH1R * AH1Inv;
                        TempBAT<v2_h1_select_t, v2_t2_select_t> * bat = nullptr;
                        if (reencode) {
                            typedef typename TempBAT<v2_h1_select_t, v2_t2_select_t>::coldesc_head_t bat_coldesc_head_t;
                            typedef typename TempBAT<v2_h1_select_t, v2_t2_select_t>::coldesc_tail_t bat_coldesc_tail_t;
                            bat = new TempBAT<v2_h1_select_t, v2_t2_select_t>(
                                    bat_coldesc_head_t(ColumnMetaData(arg1->head.metaData.width, AH1R, AH1InvR, arg1->head.metaData.AN_unencMaxU, arg1->head.metaData.AN_unencMinS)),
                                    bat_coldesc_tail_t(
                                            ColumnMetaData(arg2->tail.metaData.width, arg2->tail.metaData.AN_A, arg2->tail.metaData.AN_Ainv, arg2->tail.metaData.AN_unencMaxU,
                                                    arg2->tail.metaData.AN_unencMinS)));
                        } else {
                            bat = skeletonJoin<v2_h1_select_t, v2_t2_select_t>(arg1, arg2);
                        }
                        std::vector<bool> *vec1 = (isHead1Encoded ? new std::vector<bool>(arg1->size()) : nullptr);
                        std::vector<bool> *vec2 = (isTail1Encoded ? new std::vector<bool>(arg1->size()) : nullptr);
                        std::vector<bool> *vec3 = (isHead2Encoded ? new std::vector<bool>(arg2->size()) : nullptr);
                        std::vector<bool> *vec4 = nullptr;
                        auto iter1 = arg1->begin();
                        auto iter2 = arg2->begin();
                        size_t pos1 = 0;
                        size_t pos2 = 0;
                        while (iter1->hasNext() && iter2->hasNext()) {
                            head2_t h2 = isHead2Encoded ? static_cast<head2_t>(iter2->head() * AH2Inv) : iter2->head();
                            if (isHead2Encoded && (h2 > AH2UnencMaxU)) {
                                (*vec3)[pos2] = true;
                            }
                            tail1_t t1 = isTail1Encoded ? static_cast<tail1_t>(iter1->tail() * AT1Inv) : iter1->tail();
                            for (; iter1->hasNext() && t1 < h2; ++*iter1, ++pos1, t1 = isTail1Encoded ? (iter1->tail() * AT1Inv) : iter1->tail()) {
                                if (isHead1Encoded && (static_cast<head1_t>(iter1->head() * AH1Inv) > AH1UnencMaxU)) {
                                    (*vec1)[pos1] = true;
                                }
                                if (isTail1Encoded && (t1 > AT1UnencMaxU)) {
                                    (*vec2)[pos1] = true;
                                }
                            }
                            for (; iter2->hasNext() && t1 > h2; ++*iter2, ++pos2, h2 = isHead2Encoded ? (iter2->head() * AH2Inv) : iter2->head()) {
                                if (isHead2Encoded && (h2 > AH2UnencMaxU)) {
                                    (*vec3)[pos2] = true;
                                }
                            }
                            if (t1 == h2) {
                                if (reencode) {
                                    bat->append(std::make_pair(iter1->head() * reencFactorH1, iter2->tail()));
                                } else {
                                    bat->append(std::make_pair(iter1->head(), iter2->tail()));
                                }
                                ++*iter1;
                                ++pos1;
                                ++*iter2;
                                ++pos2;
                            }
                        }

                        delete iter1;
                        delete iter2;
                        return make_tuple(bat, vec1, vec2, vec3, vec4);
                    }
                };

                template<typename Head1, typename Tail2, bool reencode>
                struct MatchjoinAN<Head1, v2_void_t, v2_void_t, Tail2, reencode> {

                    typedef typename Head1::type_t head1_t;
                    typedef typename Tail2::type_t tail2_t;
                    typedef typename ReturnTypeSelector<Head1, reencode>::v2_select_t v2_h1_select_t;
                    typedef typename ReturnTypeSelector<Tail2, reencode>::v2_select_t v2_t2_select_t;
                    typedef typename TypeMap<Head1>::v2_encoded_t H1Enc;
                    typedef typename H1Enc::type_t h1enc_t;
                    typedef typename TypeMap<v2_void_t>::v2_encoded_t T1Enc;
                    typedef typename T1Enc::type_t t1enc_t;
                    typedef typename TypeMap<v2_void_t>::v2_encoded_t H2Enc;
                    typedef typename H2Enc::type_t h2enc_t;
                    typedef typename TypeMap<Tail2>::v2_encoded_t T2Enc;
                    typedef typename T2Enc::type_t t2enc_t;
                    typedef typename TypeMap<Head1>::v2_base_t::type_t h1unenc_t;
                    typedef typename TypeMap<v2_void_t>::v2_base_t::type_t t1unenc_t;
                    typedef typename TypeMap<v2_void_t>::v2_base_t::type_t h2unenc_t;
                    typedef typename TypeMap<Tail2>::v2_base_t::type_t t2unenc_t;
                    typedef typename ahead::larger_type<t1unenc_t, h2unenc_t>::type_t larger_t;

                    static std::tuple<BAT<v2_h1_select_t, v2_t2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> run(
                            BAT<Head1, v2_void_t>* arg1,
                            BAT<v2_void_t, Tail2>* arg2,
                            h1enc_t AH1R = h1enc_t(1), // for reencode
                            h1enc_t AH1InvR = h1enc_t(1), // for reencode
                            t2enc_t AT2R = t2enc_t(1), // for reencode
                            t2enc_t AT2InvR = t2enc_t(1) // for reencode
                                    ) {
                        constexpr const bool isHead1Encoded = std::is_base_of<v2_anencoded_t, Head1>::value;
                        constexpr const bool isTail2Encoded = std::is_base_of<v2_anencoded_t, Tail2>::value;
                        const h1enc_t AH1Inv = isHead1Encoded ? arg1->head.metaData.AN_Ainv : 1;
                        const h1enc_t AH1UnencMaxU = arg1->head.metaData.AN_unencMaxU;
                        const t2enc_t AT2Inv = isTail2Encoded ? arg2->tail.metaData.AN_Ainv : 1;
                        const t2enc_t AT2UnencMaxU = arg2->tail.metaData.AN_unencMaxU;
                        // do we need any conversion between left Tail and right Head? If so, also regard which of the types is larger
                        const h1enc_t reencFactorH1 = AH1R * AH1Inv;
                        const t2enc_t reencFactorT2 = AT2R * AT2Inv;
                        TempBAT<v2_h1_select_t, v2_t2_select_t> * bat = nullptr;
                        if (reencode) {
                            typedef typename TempBAT<v2_h1_select_t, v2_t2_select_t>::coldesc_head_t bat_coldesc_head_t;
                            typedef typename TempBAT<v2_h1_select_t, v2_t2_select_t>::coldesc_tail_t bat_coldesc_tail_t;
                            bat = new TempBAT<v2_h1_select_t, v2_t2_select_t>(
                                    bat_coldesc_head_t(ColumnMetaData(arg1->head.metaData.width, AH1R, AH1InvR, arg1->head.metaData.AN_unencMaxU, arg1->head.metaData.AN_unencMinS)),
                                    bat_coldesc_tail_t(ColumnMetaData(arg2->tail.metaData.width, AT2R, AT2InvR, arg2->tail.metaData.AN_unencMaxU, arg2->tail.metaData.AN_unencMinS)));
                        } else {
                            bat = skeletonJoin<v2_h1_select_t, v2_t2_select_t>(arg1, arg2);
                        }
                        std::vector<bool> *vec1 = (isHead1Encoded ? new std::vector<bool>(arg1->size()) : nullptr);
                        std::vector<bool> *vec4 = (isTail2Encoded ? new std::vector<bool>(arg2->size()) : nullptr);
                        auto iter1 = arg1->begin();
                        auto iter2 = arg2->begin();

                        size_t pos1 = 0;
                        size_t pos2 = 0;
                        if (iter1->hasNext() && iter2->hasNext()) {
                            if (arg1->tail.metaData.seqbase < arg2->head.metaData.seqbase) {
                                for (auto x = arg2->head.metaData.seqbase; iter1->hasNext() && iter2->hasNext() && iter1->tail() < x; ++*iter1, ++pos1) {
                                    if (isHead1Encoded && (static_cast<head1_t>(iter1->head() * AH1Inv) > AH1UnencMaxU)) {
                                        (*vec1)[pos1] = true;
                                    }
                                }
                            } else if (arg1->tail.metaData.seqbase > arg2->head.metaData.seqbase) {
                                for (auto x = arg1->tail.metaData.seqbase; iter1->hasNext() && iter2->hasNext() && iter2->head() < x; ++*iter2, ++pos2) {
                                    if (isTail2Encoded && (static_cast<tail2_t>(iter2->tail() * AT2Inv) > AT2UnencMaxU)) {
                                        (*vec4)[pos2] = true;
                                    }
                                }
                            }
                            for (; iter1->hasNext() && iter2->hasNext(); ++*iter1, ++*iter2, ++pos1, ++pos2) {
                                if (isHead1Encoded && (static_cast<head1_t>(iter1->head() * AH1Inv) > AH1UnencMaxU)) {
                                    (*vec1)[pos1] = true;
                                }
                                if (isTail2Encoded && (static_cast<tail2_t>(iter2->tail() * AT2Inv) > AT2UnencMaxU)) {
                                    (*vec4)[pos2] = true;
                                }
                                if (reencode) {
                                    bat->append(std::make_pair(iter1->head() * reencFactorH1, iter2->tail() * reencFactorT2));
                                } else {
                                    bat->append(std::make_pair(iter1->head(), iter2->tail()));
                                }
                            }
                        }

                        delete iter1;
                        delete iter2;
                        return make_tuple(bat, vec1, nullptr, nullptr, vec4);
                    }
                };

                template<typename T2, bool reencode>
                struct FetchjoinAN {
                    typedef v2_void_t ReturnHead;
                    typedef typename TypeMap<T2>::v2_encoded_t TailEncoded;
                    typedef typename TailEncoded::type_t tailenc_t;
                    typedef typename TailEncoded::v2_select_t ReturnTail;

                    static std::tuple<BAT<ReturnHead, ReturnTail> *, AN_indicator_vector *, AN_indicator_vector *> run(
                            BAT<v2_void_t, v2_resoid_t> *arg1,
                            BAT<v2_void_t, T2> *arg2,
                            resoid_t AOID,
                            tailenc_t ATReenc = tailenc_t(1),
                            tailenc_t ATReencInv = tailenc_t(1)) {
                        constexpr const bool isTailEncoded = std::is_base_of<v2_anencoded_t, T2>::value;
                        const resoid_t AT1inv = static_cast<resoid_t>(arg1->tail.metaData.AN_Ainv);
                        const tailenc_t AT2inv = static_cast<tailenc_t>(arg2->tail.metaData.AN_Ainv);
                        const tailenc_t AT2unencMaxU = static_cast<tailenc_t>(arg2->tail.metaData.AN_unencMaxU);
                        tailenc_t AT2Reenc(1);
                        auto result = skeletonJoin<ReturnHead, ReturnTail>(arg1, arg2);
                        if (reencode) {
                            result->tail.metaData.AN_A = ATReenc;
                            result->tail.metaData.AN_Ainv = ATReencInv;
                            const tailenc_t AT2invR = v2convert<tailenc_t>(ext_euclidean(uint128_t(arg2->tail.metaData.AN_A), sizeof(tailenc_t) * 8));
                            AT2Reenc = isTailEncoded ? (AT2invR * ATReenc) : ATReenc;
                        }
                        result->reserve(arg1->size());
                        auto vec1 = new AN_indicator_vector;
                        vec1->reserve(32);
                        auto vec2 = isTailEncoded ? new AN_indicator_vector : nullptr;
                        if (isTailEncoded) {
                            vec2->reserve(32);
                        }
                        auto iter1 = arg1->begin();
                        auto vec = arg2->tail.container.get();
                        const auto vecSize = vec->size();
                        for (size_t i = 0; iter1->hasNext(); ++*iter1, ++i) {
                            resoid_t t1 = static_cast<resoid_t>(iter1->tail() * AT1inv);
                            if (t1 > vecSize) {
                                // we can even further restrict the set of possible codewords here!
                                vec1->push_back(static_cast<resoid_t>(i) * AOID);
                            } else {
                                tailenc_t t2 = static_cast<tailenc_t>((*vec)[t1]);
                                if (isTailEncoded && ((t2 * AT2inv) > AT2unencMaxU)) {
                                    vec2->push_back(t1 * AOID);
                                } else if (reencode) {
                                    result->append(t2 * AT2Reenc);
                                } else {
                                    result->append(t2);
                                }
                            }
                        }
                        delete iter1;
                        return std::make_tuple(result, vec1, vec2);
                    }
                };

                template<bool reencode>
                struct FetchjoinAN<v2_str_t, reencode> {

                    static std::tuple<BAT<v2_void_t, v2_str_t> *, AN_indicator_vector *, AN_indicator_vector *> run(
                            BAT<v2_void_t, v2_resoid_t> *arg1,
                            BAT<v2_void_t, v2_str_t> *arg2,
                            resoid_t AOID) {
                        const resoid_t AT1inv = static_cast<resoid_t>(arg1->tail.metaData.AN_Ainv);
                        auto result = skeletonJoin<v2_void_t, v2_str_t>(arg1, arg2);
                        result->reserve(arg1->size());
                        auto vec1 = new AN_indicator_vector;
                        vec1->reserve(32);
                        auto iter1 = arg1->begin();
                        auto vec = arg2->tail.container.get();
                        const auto vecSize = vec->size();
                        for (size_t i = 0; iter1->hasNext(); ++*iter1, ++i) {
                            resoid_t t1 = static_cast<resoid_t>(iter1->tail() * AT1inv);
                            if (t1 > vecSize) {
                                // we can even further restrict the set of possible codewords here!
                                vec1->push_back(static_cast<resoid_t>(i) * AOID);
                            } else {
                                result->append((*vec)[t1]);
                            }
                        }
                        delete iter1;
                        return std::make_tuple(result, vec1, nullptr);
                    }
                };

            }

            template<typename H1, typename T1, typename H2, typename T2>
            std::tuple<BAT<typename H1::v2_select_t, typename T2::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> matchjoinAN(
                    BAT<H1, T1> *arg1,
                    BAT<H2, T2> *arg2) {
                return Private::MatchjoinAN<H1, T1, H2, T2, false>::run(arg1, arg2);
            }

            template<typename H1, typename T1, typename H2, typename T2>
            std::tuple<BAT<typename H1::v2_select_t, typename T2::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> matchjoinAN(
                    BAT<H1, T1> *arg1,
                    BAT<H2, T2> *arg2,
                    typename TypeMap<H1>::v2_encoded_t::type_t AH1reenc,
                    typename TypeMap<H1>::v2_encoded_t::type_t AH1InvReenc) {
                return Private::MatchjoinAN<H1, T1, H2, T2, true>::run(arg1, arg2, AH1reenc, AH1InvReenc);
            }

            template<typename H1, typename T1, typename H2, typename T2>
            std::tuple<BAT<typename H1::v2_select_t, typename T2::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> matchjoinAN(
                    BAT<H1, T1> *arg1,
                    BAT<H2, T2> *arg2,
                    typename TypeMap<H1>::v2_encoded_t::type_t AH1reenc,
                    typename TypeMap<H1>::v2_encoded_t::type_t AH1InvReenc,
                    typename TypeMap<T2>::v2_encoded_t::type_t AT2Reenc,
                    typename TypeMap<T2>::v2_encoded_t::type_t AT2InvReenc) {
                return Private::MatchjoinAN<H1, T1, H2, T2, true>::run(arg1, arg2, AH1reenc, AH1InvReenc, AT2Reenc, AT2InvReenc);
            }

            template<typename T2>
            std::tuple<BAT<v2_void_t, typename TypeMap<T2>::v2_encoded_t> *, AN_indicator_vector *, AN_indicator_vector *> fetchjoinAN(
                    BAT<v2_void_t, v2_resoid_t> *arg1,
                    BAT<v2_void_t, T2> *arg2,
                    resoid_t AOID) {
                return Private::FetchjoinAN<T2, false>::run(arg1, arg2, AOID);
            }

            template<typename T2>
            std::tuple<BAT<v2_void_t, typename TypeMap<T2>::v2_encoded_t> *, AN_indicator_vector *, AN_indicator_vector *> fetchjoinAN(
                    BAT<v2_void_t, v2_resoid_t> *arg1,
                    BAT<v2_void_t, T2> *arg2,
                    typename TypeMap<T2>::v2_encoded_t::type_t ATReenc,
                    typename TypeMap<T2>::v2_encoded_t::type_t ATReencInv,
                    resoid_t AOID) {
                return Private::FetchjoinAN<T2, true>::run(arg1, arg2, AOID, ATReenc, ATReencInv);
            }

        }
    }
}

#endif /* MATCHJOIN_AN_TCC */
