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
#include "ANhelper.tcc"
#include <util/v2typeconversion.hpp>

using boost::multiprecision::uint128_t;

namespace ahead {
    namespace bat {
        namespace ops {

            namespace Private {

                template<typename Head1, typename Tail1, typename Head2, typename Tail2, bool reencode>
                struct MatchjoinAN {

                    typedef ANhelper<Head1> Head1Helper;
                    typedef ANhelper<Tail1> Tail1Helper;
                    typedef ANhelper<Head2> Head2Helper;
                    typedef ANhelper<Tail2> Tail2Helper;
                    typedef typename ANReturnTypeSelector<Head1, reencode>::v2_type_t v2_h1_t;
                    typedef typename ANReturnTypeSelector<Tail2, reencode>::v2_type_t v2_t2_t;
                    typedef typename Head1Helper::type_t h1_t;
                    typedef typename Tail1Helper::type_t t1_t;
                    typedef typename Head2Helper::type_t h2_t;
                    typedef typename Tail2Helper::type_t t2_t;

                    static std::tuple<BAT<v2_h1_t, v2_t2_t>*, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *> run(
                            BAT<Head1, Tail1>* arg1,
                            BAT<Head2, Tail2>* arg2,
                            resoid_t AOID,
                            h1_t AH1R = h1_t(0), // for reencode
                            h1_t AH1InvR = h1_t(0), // for reencode
                            t2_t AT2R = h2_t(0), // for reencode
                            t2_t AT2InvR = h2_t(0)) { // for reencode
                        h1_t const AH1Inv = Head1Helper::getIfEncoded(arg1->head.metaData.AN_Ainv);
                        h1_t const AH1UnencMaxU = Head1Helper::getIfEncoded(arg1->head.metaData.AN_unencMaxU);
                        t1_t const AT1Inv = Tail1Helper::getIfEncoded(arg1->tail.metaData.AN_Ainv);
                        t1_t const AT1UnencMaxU = Tail1Helper::getIfEncoded(arg1->tail.metaData.AN_unencMaxU);
                        h2_t const AH2Inv = Head2Helper::getIfEncoded(arg2->head.metaData.AN_Ainv);
                        h2_t const AH2UnencMaxU = Head2Helper::getIfEncoded(arg2->head.metaData.AN_unencMaxU);
                        t2_t const AT2Inv = Tail2Helper::getIfEncoded(arg2->tail.metaData.AN_Ainv);
                        t2_t const AT2UnencMaxU = Tail2Helper::getIfEncoded(arg2->tail.metaData.AN_unencMaxU);
                        // do we need any conversion between left Tail and right Head? If so, also regard which of the types is larger
                        h1_t const reencFactorH1 = Head1Helper::mulIfEncoded(AH1R, AH1Inv);
                        t2_t const reencFactorT2 = Tail2Helper::mulIfEncoded(AT2R, AT2Inv);
                        TempBAT<v2_h1_t, v2_t2_t> * bat = nullptr;
                        if (reencode) {
                            typedef typename TempBAT<v2_h1_t, v2_t2_t>::coldesc_head_t bat_coldesc_head_t;
                            typedef typename TempBAT<v2_h1_t, v2_t2_t>::coldesc_tail_t bat_coldesc_tail_t;
                            bat = new TempBAT<v2_h1_t, v2_t2_t>(
                                    bat_coldesc_head_t(ColumnMetaData(arg1->head.metaData.width, AH1R, AH1InvR, arg1->head.metaData.AN_unencMaxU, arg1->head.metaData.AN_unencMinS)),
                                    bat_coldesc_tail_t(ColumnMetaData(arg2->tail.metaData.width, AT2R, AT2InvR, arg2->tail.metaData.AN_unencMaxU, arg2->tail.metaData.AN_unencMinS)));
                        } else {
                            bat = skeletonJoin<v2_h1_t, v2_t2_t>(arg1, arg2);
                        }
                        bat->reserve(arg1->size());
                        AN_indicator_vector *vec1 = Head1Helper::createIndicatorVector();
                        AN_indicator_vector *vec2 = Tail1Helper::createIndicatorVector();
                        AN_indicator_vector *vec3 = Head2Helper::createIndicatorVector();
                        AN_indicator_vector *vec4 = Tail2Helper::createIndicatorVector();
                        auto iter1 = arg1->begin();
                        auto iter2 = arg2->begin();
                        size_t pos1 = 0;
                        size_t pos2 = 0;
                        while (iter1->hasNext() && iter2->hasNext()) {
                            h2_t h2 = Head2Helper::mulIfEncoded(iter2->head(), AH2Inv);
                            if (Head2Helper::isEncoded && (h2 > AH2UnencMaxU)) {
                                vec3->push_back(pos2 * AOID);
                            }
                            if (Tail2Helper::isEncoded && (Tail2Helper::mulIfEncoded(iter2->tail(), AT2Inv) > AT2UnencMaxU)) {
                                vec4->push_back(pos2 * AOID);
                            }
                            t1_t t1 = Tail1Helper::mulIfEncoded(iter1->tail(), AT1Inv);
                            bool iterValid = false;
                            for (; (iterValid = iter1->hasNext()) && t1 < h2; ++*iter1, ++pos1, t1 = Tail1Helper::mulIfEncoded(iter1->tail(), AT1Inv)) {
                                if (Head1Helper::isEncoded && (Head1Helper::mulIfEncoded(iter1->head(), AH1Inv) > AH1UnencMaxU)) {
                                    vec1->push_back(pos1 * AOID);
                                }
                                if (Tail1Helper::isEncoded && (t1 > AT1UnencMaxU)) {
                                    vec2->push_back(pos1 * AOID);
                                }
                            }
                            if (iterValid) {
                                for (; (iterValid = iter2->hasNext()) && t1 > h2; ++*iter2, ++pos2, h2 = Head2Helper::mulIfEncoded(iter2->head(), AH2Inv)) {
                                    if (Head2Helper::isEncoded && (h2 > AH2UnencMaxU)) {
                                        vec3->push_back(pos2 * AOID);
                                    }
                                    if (Tail2Helper::isEncoded && (Tail2Helper::mulIfEncoded(iter2->tail(), AT2Inv) > AT2UnencMaxU)) {
                                        vec4->push_back(pos2 * AOID);
                                    }
                                }
                                if (iterValid && (t1 == h2)) {
                                    if (reencode) {
                                        bat->append(std::make_pair(Head1Helper::mulIfEncoded(iter1->head(), reencFactorH1), Tail2Helper::mulIfEncoded(iter2->tail(), reencFactorT2)));
                                    } else {
                                        bat->append(std::make_pair(iter1->head(), iter2->tail()));
                                    }
                                    ++*iter1;
                                    ++pos1;
                                    ++*iter2;
                                    ++pos2;
                                }
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
                    typedef ANhelper<Head1> Head1Helper;
                    typedef ANhelper<Tail2> Tail2Helper;
                    typedef typename ANReturnTypeSelector<Head1, reencode>::v2_type_t v2_h1_t;
                    typedef typename ANReturnTypeSelector<Tail2, reencode>::v2_type_t v2_t2_t;
                    typedef typename Head1Helper::type_t h1_t;
                    typedef typename Tail2Helper::type_t t2_t;

                    static std::tuple<BAT<v2_h1_t, v2_t2_t>*, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *> run(
                            BAT<Head1, v2_void_t>* arg1,
                            BAT<v2_void_t, Tail2>* arg2,
                            resoid_t AOID,
                            h1_t AH1R = typename Head1Helper::type_t(0), // for reencode
                            h1_t AH1InvR = typename Head1Helper::type_t(0), // for reencode
                            t2_t AT2R = typename Tail2Helper::type_t(0), // for reencode
                            t2_t AT2InvR = typename Tail2Helper::type_t(0)) { // for reencode
                        h1_t const AH1Inv = Head1Helper::getIfEncoded(arg1->head.metaData.AN_Ainv);
                        h1_t const AH1UnencMaxU = Head1Helper::getIfEncoded(arg1->head.metaData.AN_unencMaxU);
                        t2_t const AT2Inv = Tail2Helper::getIfEncoded(arg2->tail.metaData.AN_Ainv);
                        t2_t const AT2UnencMaxU = Tail2Helper::getIfEncoded(arg2->tail.metaData.AN_unencMaxU);
                        h1_t const reencFactorH1 = Head1Helper::mulIfEncoded(AH1R, AH1Inv);
                        t2_t const reencFactorT2 = Tail2Helper::mulIfEncoded(AT2R, AT2Inv);
                        TempBAT<v2_h1_t, v2_t2_t> * bat = nullptr;
                        if (reencode) {
                            typedef typename TempBAT<v2_h1_t, v2_t2_t>::coldesc_head_t bat_coldesc_head_t;
                            typedef typename TempBAT<v2_h1_t, v2_t2_t>::coldesc_tail_t bat_coldesc_tail_t;
                            bat = new TempBAT<v2_h1_t, v2_t2_t>(
                                    bat_coldesc_head_t(ColumnMetaData(arg1->head.metaData.width, AH1R, AH1InvR, arg1->head.metaData.AN_unencMaxU, arg1->head.metaData.AN_unencMinS)),
                                    bat_coldesc_tail_t(ColumnMetaData(arg2->tail.metaData.width, AT2R, AT2InvR, arg2->tail.metaData.AN_unencMaxU, arg2->tail.metaData.AN_unencMinS)));
                        } else {
                            bat = skeletonJoin<v2_h1_t, v2_t2_t>(arg1, arg2);
                        }
                        bat->reserve(arg1->size());
                        AN_indicator_vector *vec1 = Head1Helper::createIndicatorVector();
                        AN_indicator_vector *vec4 = Tail2Helper::createIndicatorVector();
                        auto iter1 = arg1->begin();
                        auto iter2 = arg2->begin();

                        size_t pos1 = 0;
                        size_t pos2 = 0;
                        if (iter1->hasNext() && iter2->hasNext()) {
                            if (arg1->tail.metaData.seqbase < arg2->head.metaData.seqbase) {
                                for (auto x = arg2->head.metaData.seqbase; iter1->hasNext() && iter2->hasNext() && iter1->tail() < x; ++*iter1, ++pos1) {
                                    if (Head1Helper::isEncoded && (static_cast<head1_t>(iter1->head() * AH1Inv) > AH1UnencMaxU)) {
                                        vec1->push_back(pos1 * AOID);
                                    }
                                }
                            } else if (arg1->tail.metaData.seqbase > arg2->head.metaData.seqbase) {
                                for (auto x = arg1->tail.metaData.seqbase; iter1->hasNext() && iter2->hasNext() && iter2->head() < x; ++*iter2, ++pos2) {
                                    if (Tail2Helper::isEncoded && (static_cast<tail2_t>(iter2->tail() * AT2Inv) > AT2UnencMaxU)) {
                                        vec4->push_back(pos2 * AOID);
                                    }
                                }
                            }
                            for (; iter1->hasNext() && iter2->hasNext(); ++*iter1, ++*iter2, ++pos1, ++pos2) {
                                if (Head1Helper::isEncoded && (static_cast<head1_t>(iter1->head() * AH1Inv) > AH1UnencMaxU)) {
                                    vec1->push_back(pos1 * AOID);
                                }
                                if (Tail2Helper::isEncoded && (static_cast<tail2_t>(iter2->tail() * AT2Inv) > AT2UnencMaxU)) {
                                    vec4->push_back(pos2 * AOID);
                                }
                                if (reencode) {
                                    bat->append(std::make_pair(Head1Helper::mulIfEncoded(iter1->head(), reencFactorH1), Tail2Helper::mulIfEncoded(iter2->tail(), reencFactorT2)));
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

                template<typename Head1, typename Tail2, bool reencode>
                struct MatchjoinAN<Head1, v2_resoid_t, v2_void_t, Tail2, reencode> {

                    typedef ANhelper<Head1> Head1Helper;
                    typedef ANhelper<v2_resoid_t> Tail1Helper;
                    typedef ANhelper<v2_void_t> Head2Helper;
                    typedef ANhelper<Tail2> Tail2Helper;
                    typedef typename ANReturnTypeSelector<Head1, reencode>::v2_type_t v2_h1_t;
                    typedef typename ANReturnTypeSelector<Tail2, reencode>::v2_type_t v2_t2_t;
                    typedef typename Head1Helper::type_t h1_t;
                    typedef typename Tail1Helper::type_t t1_t;
                    typedef typename Head2Helper::type_t h2_t;
                    typedef typename Tail2Helper::type_t t2_t;

                    static std::tuple<BAT<v2_h1_t, v2_t2_t>*, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *> run(
                            BAT<Head1, v2_resoid_t>* arg1,
                            BAT<v2_void_t, Tail2>* arg2,
                            resoid_t AOID,
                            h1_t AH1R = h1_t(0), // for reencode
                            h1_t AH1InvR = h1_t(0), // for reencode
                            t2_t AT2R = t2_t(0), // for reencode
                            t2_t AT2InvR = t2_t(0)) { // for reencode
                        h1_t const AH1Inv = Head1Helper::getIfEncoded(arg1->head.metaData.AN_Ainv);
                        h1_t const AH1UnencMaxU = Head1Helper::getIfEncoded(arg1->head.metaData.AN_unencMaxU);
                        t1_t const AT1Inv = Tail1Helper::getIfEncoded(arg1->tail.metaData.AN_Ainv);
                        t2_t const AT2Inv = Tail2Helper::getIfEncoded(arg2->tail.metaData.AN_Ainv);
                        t2_t const AT2UnencMaxU = Tail2Helper::getIfEncoded(arg2->tail.metaData.AN_unencMaxU);
                        // do we need any conversion between left Tail and right Head? If so, also regard which of the types is larger
                        h1_t const reencFactorH1 = Head1Helper::mulIfEncoded(AH1R, AH1Inv);
                        t2_t const reencFactorT2 = Tail2Helper::mulIfEncoded(AT2R, AT2Inv);
                        TempBAT<v2_h1_t, v2_t2_t> * bat = nullptr;
                        if (reencode) {
                            typedef typename TempBAT<v2_h1_t, v2_t2_t>::coldesc_head_t bat_coldesc_head_t;
                            typedef typename TempBAT<v2_h1_t, v2_t2_t>::coldesc_tail_t bat_coldesc_tail_t;
                            bat = new TempBAT<v2_h1_t, v2_t2_t>(
                                    bat_coldesc_head_t(ColumnMetaData(arg1->head.metaData.width, AH1R, AH1InvR, arg1->head.metaData.AN_unencMaxU, arg1->head.metaData.AN_unencMinS)),
                                    bat_coldesc_tail_t(
                                            ColumnMetaData(arg2->tail.metaData.width, ANhelper<Tail2, uint16_t>::getIfEncoded(AT2R), ANhelper<Tail2, uint64_t>::getIfEncoded(AT2InvR),
                                                    arg2->tail.metaData.AN_unencMaxU, arg2->tail.metaData.AN_unencMinS)));
                        } else {
                            bat = skeletonJoin<v2_h1_t, v2_t2_t>(arg1, arg2);
                        }
                        bat->reserve(arg1->size());
                        AN_indicator_vector *vec1 = Head1Helper::createIndicatorVector();
                        AN_indicator_vector *vec2 = Tail1Helper::createIndicatorVector();
                        AN_indicator_vector *vec4 = Tail2Helper::createIndicatorVector();
                        auto iter1 = arg1->begin();
                        auto vec = arg2->tail.container.get();
                        const resoid_t szArg2 = arg2->size();
                        for (size_t pos1 = 0; iter1->hasNext(); ++*iter1, ++pos1) {
                            h1_t h1 = iter1->head();
                            if (Head1Helper::isEncoded && ((h1 * AH1Inv) > AH1UnencMaxU)) {
                                vec1->push_back(pos1 * AOID);
                            }
                            t1_t t1 = iter1->tail();
                            resoid_t pos2 = t1 * AT1Inv;
                            if (pos2 > szArg2) {
                                vec2->push_back(pos1 * AOID);
                            } else {
                                t2_t t2 = (*vec)[pos2];
                                if (Tail2Helper::isEncoded && (Tail2Helper::mulIfEncoded(t2, AT2Inv) > AT2UnencMaxU)) {
                                    vec4->push_back(pos2 * AOID);
                                } else {
                                    if (reencode) {
                                        bat->append(std::make_pair(Head1Helper::mulIfEncoded(h1, reencFactorH1), Tail2Helper::mulIfEncoded(t2, reencFactorT2)));
                                    } else {
                                        bat->append(std::make_pair(h1, t2));
                                    }
                                }
                            }
                        }

                        delete iter1;
                        return make_tuple(bat, vec1, vec2, nullptr, vec4);
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
            std::tuple<BAT<typename H1::v2_select_t, typename T2::v2_select_t>*, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *> matchjoinAN(
                    BAT<H1, T1> *arg1,
                    BAT<H2, T2> *arg2,
                    resoid_t AOID) {
                return Private::MatchjoinAN<H1, T1, H2, T2, false>::run(arg1, arg2, AOID);
            }

            template<typename H1, typename T1, typename H2, typename T2>
            std::tuple<BAT<typename H1::v2_select_t, typename T2::v2_select_t>*, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *> matchjoinAN(
                    BAT<H1, T1> *arg1,
                    BAT<H2, T2> *arg2,
                    typename TypeMap<H1>::v2_encoded_t::type_t AH1reenc,
                    typename TypeMap<H1>::v2_encoded_t::type_t AH1InvReenc,
                    resoid_t AOID) {
                return Private::MatchjoinAN<H1, T1, H2, T2, true>::run(arg1, arg2, AOID, AH1reenc, AH1InvReenc);
            }

            template<typename H1, typename T1, typename H2, typename T2>
            std::tuple<BAT<typename H1::v2_select_t, typename T2::v2_select_t>*, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *> matchjoinAN(
                    BAT<H1, T1> *arg1,
                    BAT<H2, T2> *arg2,
                    typename TypeMap<H1>::v2_encoded_t::type_t AH1reenc,
                    typename TypeMap<H1>::v2_encoded_t::type_t AH1InvReenc,
                    typename TypeMap<T2>::v2_encoded_t::type_t AT2Reenc,
                    typename TypeMap<T2>::v2_encoded_t::type_t AT2InvReenc,
                    resoid_t AOID) {
                return Private::MatchjoinAN<H1, T1, H2, T2, true>::run(arg1, arg2, AOID, AH1reenc, AH1InvReenc, AT2Reenc, AT2InvReenc);
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
