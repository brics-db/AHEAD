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
 * File:   operatorsAN.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 9. August 2016, 13:12
 */

#ifndef OPERATORSAN_TCC
#define OPERATORSAN_TCC

#include <sstream>
#include <type_traits>

#include <ColumnStore.h>
#include <column_storage/Bat.h>
#include <column_storage/TempBat.h>
#include <util/resilience.hpp>

#include "operators.h"

namespace v2 {
    namespace bat {
        namespace ops {

            template<typename Head, typename Tail>
            Bat<Head, typename TypeMap<Tail>::v2_encoded_t>* encode_AN(Bat<Head, Tail>* arg, typename TypeMap<Tail>::v2_encoded_t::type_t A = TypeMap<Tail>::v2_encoded_t::A, size_t start = 0, size_t size = 0) {
                typedef typename TypeMap<Tail>::v2_encoded_t::type_t tail_t;
                static_assert(is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                static_assert(is_base_of<v2_base_t, Tail>::value, "Tail must be a base type");
                auto result = new TempBat<Head, typename TypeMap<Tail>::v2_encoded_t > (arg->size());
                auto iter = arg->begin();
                if (iter->hasNext()) {
                    if (start)
                        iter->position(start);
                    result->append(make_pair(move(iter->head()), move(static_cast<tail_t> (iter->tail()) * A)));
                    if (size) {
                        for (size_t step = 1; step < size && iter->hasNext(); ++step, ++*iter) {
                            result->append(make_pair(move(iter->head()), move(static_cast<tail_t> (iter->tail()) * A)));
                        }
                    } else {
                        for (; iter->hasNext(); ++*iter) {
                            result->append(make_pair(move(iter->head()), move(static_cast<tail_t> (iter->tail()) * A)));
                        }
                    }
                }
                delete iter;
                return result; // possibly empty
            }

            template<typename Head, typename ResTail>
            vector<bool>* check_AN(Bat<Head, ResTail>* arg, typename ResTail::type_t aInv = ResTail::A_INV, typename ResTail::type_t unEncMaxU = ResTail::A_UNENC_MAX_U, size_t start = 0, size_t size = 0) {
                static_assert(is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                static_assert(is_base_of<v2_anencoded_t, ResTail>::value, "ResTail must be an AN-encoded type");
                auto result = new vector<bool>();
                result->reserve(arg->size());
                auto iter = arg->begin();
                if (iter->hasNext()) {
                    if (start)
                        iter->position(start);
                    result->emplace_back(((iter->tail()) * aInv) <= unEncMaxU);
                    if (size) {
                        for (size_t step = 1; step < size && iter->hasNext(); ++step, ++*iter) {
                            result->emplace_back((iter->tail() * aInv) <= unEncMaxU);
                        }
                    } else {
                        for (; iter->hasNext(); ++*iter) {
                            result->emplace_back((iter->tail() * aInv) <= unEncMaxU);
                        }
                    }
                }
                delete iter;
                return result; // possibly empty
            }

            template<typename Head, typename ResTail>
            Bat<Head, typename ResTail::unenc_v2_t>* decode_AN(Bat<Head, ResTail>* arg, typename ResTail::type_t aInv = ResTail::A_INV, typename ResTail::type_t unEncMaxU = ResTail::A_UNENC_MAX_U, size_t start = 0, size_t size = 0) {
                static_assert(is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                static_assert(is_base_of<v2_anencoded_t, ResTail>::value, "ResTail must be an AN-encoded type");
                typedef typename ResTail::unenc_v2_t Tail;
                typedef typename Tail::type_t tail_t;
                auto result = new TempBat<Head, Tail>(arg->size());
                auto iter = arg->begin();
                if (iter->hasNext()) {
                    if (start)
                        iter->position(start);
                    result->append(make_pair(move(iter->head()), move(static_cast<tail_t> (iter->tail() * aInv))));
                    if (size) {
                        for (size_t step = 1; step < size && iter->hasNext(); ++step, ++*iter) {
                            result->append(make_pair(move(iter->head()), move(static_cast<tail_t> (iter->tail() * aInv))));
                        }
                    } else {
                        for (; iter->hasNext(); ++*iter) {
                            result->append(make_pair(move(iter->head()), move(static_cast<tail_t> (iter->tail() * aInv))));
                        }
                    }
                }
                delete iter;
                return result;
            }

            template<typename Head, typename ResTail>
            pair<Bat<Head, typename ResTail::unenc_v2_t>*, vector<bool>*> checkAndDecode_AN(Bat<Head, ResTail>* arg, typename ResTail::type_t aInv = ResTail::A_INV, typename ResTail::type_t aUnencMaxU = ResTail::A_UNENC_MAX_U, size_t start = 0, size_t size = 0) {
                static_assert(is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                static_assert(is_base_of<v2_anencoded_t, ResTail>::value, "ResTail must be an AN-encoded type");
                size_t sizeBAT = arg->size();
                auto result = make_pair(new TempBat<Head, typename ResTail::unenc_v2_t > (sizeBAT), new vector<bool>());
                result.second->reserve(sizeBAT);
                auto iter = arg->begin();
                if (iter->hasNext()) {
                    if (start)
                        iter->position(start);
                    typename ResTail::type_t dec = iter->tail() * aInv;
                    result.first->append(make_pair(move(iter->head()), move(static_cast<typename ResTail::unenc_v2_t::type_t> (dec))));
                    result.second->emplace_back(move(dec <= aUnencMaxU));
                    if (size) {
                        for (size_t step = 1; step < size && iter->hasNext(); ++step, ++*iter) {
                            dec = iter->tail() * aInv;
                            result.first->append(make_pair(move(iter->head()), move(static_cast<typename ResTail::unenc_v2_t::type_t> (dec))));
                            result.second->emplace_back(move(dec <= aUnencMaxU));
                        }
                    } else {
                        for (; iter->hasNext(); ++*iter) {
                            dec = iter->tail() * aInv;
                            result.first->append(make_pair(move(iter->head()), move(static_cast<typename ResTail::unenc_v2_t::type_t> (dec))));
                            result.second->emplace_back(move(dec <= aUnencMaxU));
                        }
                    }
                }
                delete iter;
                return result;
            }

            template<typename Head, typename ResTail, typename Op>
            struct selection_AN_priv {

                pair<Bat<Head, ResTail>*, vector<bool>*> operator()(Bat<Head, ResTail>* arg, typename ResTail::type_t threshold, typename ResTail::type_t aInv = ResTail::A_INV, typename ResTail::type_t unEncMaxU = ResTail::A_UNENC_MAX_U) {
                    static_assert(is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                    static_assert(is_base_of<v2_anencoded_t, ResTail>::value, "ResTail must be an AN-encoded type");
                    auto result = make_pair(new TempBat<Head, ResTail>(), new vector<bool>());
                    auto iter = arg->begin();
                    Op op;
                    for (; iter->hasNext(); ++*iter) {
                        auto t = iter->tail();
                        result.second->emplace_back((t * aInv) <= unEncMaxU);
                        if (op(t, threshold)) {
                            result.first->append(move(make_pair(move(iter->head()), move(t))));
                        }
                    }
                    delete iter;
                    return result;
                }
            };

            template<typename ResTail, typename Op>
            struct selection_AN_priv<v2_void_t, ResTail, Op> {
                typedef v2_void_t HeadSrc;
                typedef v2_oid_t HeadTrg;

                pair<Bat<HeadTrg, ResTail>*, vector<bool>*> operator()(Bat<HeadSrc, ResTail>* arg, typename ResTail::type_t threshold, typename ResTail::type_t aInv = ResTail::A_INV, typename ResTail::type_t unEncMaxU = ResTail::A_UNENC_MAX_U) {
                    static_assert(is_base_of<v2_base_t, HeadSrc>::value, "Head must be a base type");
                    static_assert(is_base_of<v2_anencoded_t, ResTail>::value, "ResTail must be an AN-encoded type");
                    auto result = make_pair(new TempBat<HeadTrg, ResTail>(), new vector<bool>());
                    auto iter = arg->begin();
                    Op op;
                    for (; iter->hasNext(); ++*iter) {
                        auto t = iter->tail();
                        result.second->emplace_back((t * aInv) <= unEncMaxU);
                        if (op(t, threshold)) {
                            result.first->append(move(make_pair(move(iter->head()), move(t))));
                        }
                    }
                    delete iter;
                    return result;
                }
            };

            template<typename Head, typename ResTail, typename Op1, typename Op2>
            struct selection_AN_priv2 {

                pair<Bat<Head, ResTail>*, vector<bool>*> operator()(Bat<Head, ResTail>* arg, typename ResTail::type_t threshold1, typename ResTail::type_t threshold2, typename ResTail::type_t aInv = ResTail::A_INV, typename ResTail::type_t unEncMaxU = ResTail::A_UNENC_MAX_U) {
                    static_assert(is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                    static_assert(is_base_of<v2_anencoded_t, ResTail>::value, "ResTail must be an AN-encoded type");
                    auto result = make_pair(new TempBat<Head, ResTail>(), new vector<bool>());
                    auto iter = arg->begin();
                    Op1 op1;
                    Op2 op2;
                    for (; iter->hasNext(); ++*iter) {
                        auto t = iter->tail();
                        result.second->emplace_back((t * aInv) <= unEncMaxU);
                        if (op1(t, threshold1) && op2(t, threshold2)) {
                            result.first->append(make_pair(move(iter->head()), move(t)));
                        }
                    }
                    delete iter;
                    return result;
                }
            };

            template<typename ResTail, typename Op1, typename Op2>
            struct selection_AN_priv2<v2_void_t, ResTail, Op1, Op2> {
                typedef v2_void_t HeadSrc;
                typedef v2_oid_t HeadTrg;

                pair<Bat<HeadTrg, ResTail>*, vector<bool>*> operator()(Bat<HeadSrc, ResTail>* arg, typename ResTail::type_t threshold1, typename ResTail::type_t threshold2, typename ResTail::type_t aInv = ResTail::A_INV, typename ResTail::type_t unEncMaxU = ResTail::A_UNENC_MAX_U) {
                    static_assert(is_base_of<v2_base_t, HeadSrc>::value, "Head must be a base type");
                    static_assert(is_base_of<v2_anencoded_t, ResTail>::value, "ResTail must be an AN-encoded type");
                    auto result = make_pair(new TempBat<HeadTrg, ResTail>(), new vector<bool>());
                    auto iter = arg->begin();
                    Op1 op1;
                    Op2 op2;
                    for (; iter->hasNext(); ++*iter) {
                        auto t = iter->tail();
                        result.second->emplace_back((t * aInv) <= unEncMaxU);
                        if (op1(t, threshold1) && op2(t, threshold2)) {
                            result.first->append(make_pair(move(iter->head()), move(t)));
                        }
                    }
                    delete iter;
                    return result;
                }
            };

            template<typename Head, typename ResTail>
            pair<Bat<v2_oid_t, ResTail>*, vector<bool>*> selection_AN(selection_type_t selType, Bat<Head, ResTail>* arg, typename ResTail::type_t threshold1, typename ResTail::type_t threshold2 = typename ResTail::type_t(0), typename ResTail::type_t aInv = ResTail::A_INV, typename ResTail::type_t unEncMaxU = ResTail::A_UNENC_MAX_U) {
                static_assert(is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                static_assert(is_base_of<v2_anencoded_t, ResTail>::value, "ResTail must be an AN-encoded type");
                if (selType == selection_type_t::LT) {
                    selection_AN_priv<Head, ResTail, std::less<typename ResTail::type_t >> impl;
                    return impl(arg, threshold1);
                } else if (selType == selection_type_t::LE) {
                    selection_AN_priv<Head, ResTail, std::less_equal<typename ResTail::type_t >> impl;
                    return impl(arg, threshold1);
                } else if (selType == selection_type_t::EQ) {
                    selection_AN_priv<Head, ResTail, std::equal_to<typename ResTail::type_t >> impl;
                    return impl(arg, threshold1);
                } else if (selType == selection_type_t::GE) {
                    selection_AN_priv<Head, ResTail, std::equal_to<typename ResTail::type_t >> impl;
                    return impl(arg, threshold1);
                } else if (selType == selection_type_t::GT) {
                    selection_AN_priv<Head, ResTail, std::equal_to<typename ResTail::type_t >> impl;
                    return impl(arg, threshold1);
                } else if (selType == selection_type_t::BT) {
                    selection_AN_priv2 <Head, ResTail, std::greater_equal<typename ResTail::type_t >, std::less_equal<typename ResTail::type_t >> impl;
                    return impl(arg, threshold1, threshold2);
                } else {
                    stringstream ss;
                    ss << "Unknown selection type \"" << selType << '"';
                    throw runtime_error(ss.str());
                }
            }

            template<typename Head, typename Tail, bool isHeadEnc>
            struct mirrorHead_AN_priv;

            template<typename Head, typename Tail>
            struct mirrorHead_AN_priv<Head, Tail, true> {

                pair<Bat<Head, Head>*, vector<bool>*> operator()(Bat<Head, Tail>* arg, typename TypeMap<Head>::v2_encoded_t::type_t A = TypeMap<Head>::v2_encoded_t::A, typename TypeMap<Head>::v2_encoded_t::type_t aInv = TypeMap<Head>::v2_encoded_t::A_INV, typename TypeMap<Head>::v2_encoded_t::type_t aUnencMaxU = TypeMap<Head>::v2_encoded_t::A_UNENC_MAX_U) {
                    static_assert(is_base_of<v2_anencoded_t, Head>::value, "Head must be an AN-encoded type");
                    size_t sizeBAT = arg->size();
                    auto result = make_pair(new TempBat<Head, Head>(sizeBAT), new vector<bool>());
                    result.second->reserve(sizeBAT);
                    auto iter = arg->begin();
                    for (; iter->hasNext(); ++*iter) {
                        auto h = iter->head();
                        result.second->emplace_back((h * aInv) < aUnencMaxU);
                        result.first->append(make_pair(h, h));
                    }
                    delete iter;
                    return result;
                }
            };

            template<typename Head, typename Tail>
            struct mirrorHead_AN_priv<Head, Tail, false> {

                pair<Bat<Head, typename TypeMap<Head>::v2_encoded_t>*, vector<bool>*> operator()(Bat<Head, Tail>* arg, typename TypeMap<Head>::v2_encoded_t::type_t A = TypeMap<Head>::v2_encoded_t::A, typename TypeMap<Head>::v2_encoded_t::type_t aInv = TypeMap<Head>::v2_encoded_t::A_INV, typename TypeMap<Head>::v2_encoded_t::type_t aUnencMaxU = TypeMap<Head>::v2_encoded_t::A_UNENC_MAX_U) {
                    typedef typename TypeMap<Head>::v2_encoded_t ResHead;
                    static_assert(is_base_of<v2_base_t, Head>::value, "Head must be an base type");
                    size_t sizeBAT = arg->size();
                    auto bat = new TempBat<Head, ResHead>(sizeBAT);
                    auto iter = arg->begin();
                    for (; iter->hasNext(); ++*iter) {
                        auto h = iter->head();
                        bat->append(make_pair(h, static_cast<typename ResHead::type_t> (h) * A));
                    }
                    delete iter;
                    return make_pair(bat, nullptr);
                }
            };

            template<typename Head, typename Tail>
            pair<Bat<typename TypeMap<Head>::v2_actual_t, typename TypeMap<Head>::v2_encoded_t>*, vector<bool>*> mirrorHead_AN(Bat<Head, Tail>* arg, typename TypeMap<Head>::v2_encoded_t::type_t A = TypeMap<Head>::v2_encoded_t::A, typename TypeMap<Head>::v2_encoded_t::type_t aInv = TypeMap<Head>::v2_encoded_t::A_INV, typename TypeMap<Head>::v2_encoded_t::type_t aUnencMaxU = TypeMap<Head>::v2_encoded_t::A_UNENC_MAX_U) {
                mirrorHead_AN_priv<Head, Tail, is_base_of<v2_anencoded_t, Head >::value> impl;
                return impl(arg, A, aInv, aUnencMaxU);
            }

            template<typename ResHead, typename ResTail>
            Bat<ResHead, ResTail>* reverse_AN(Bat<typename ResHead::unenc_v2_t, typename ResHead::unenc_v2_t>* arg, typename ResHead::type_t Ahead, typename ResTail::type_t Atail) {
                auto result = new TempBat<ResHead, ResTail>(arg->size());
                auto iter = arg->begin();
                for (; iter->hasNext(); ++*iter) {
                    result->append(make_pair(move(static_cast<typename ResTail::type_t> (iter->tail()) * Atail), move(static_cast<typename ResHead::type_t> (iter->head()) * Ahead)));
                }
                delete iter;
                return result;
            }

            /**
             * Reverses the given BAT. Will always encode the Head and leave Tail as-is.
             * When Head is already encoded, only check values. Otherwise, only encode values. The idea is that OID's are conceptually purely virtual and thus are newly instantiated. These values must be encoded.
             * The same goes for Tail.
             * Usually the Tail should already be encoded, but we'll handle the case where it is not, as well.
             * 
             * @param arg
             * @param Ahead
             * @param AtailInv
             * @param AtailUnencMaxU
             * @return 
             */
            template <typename Hin, typename Tin, typename Hout = typename TypeMap<Tin>::v2_encoded_t, typename Tout = typename TypeMap<Hin>::v2_encoded_t, typename Henc = typename TypeMap<Hin>::v2_encoded_t, typename Tenc = typename TypeMap<Tin>::v2_encoded_t>
            tuple<Bat<Hout, Tout>*, vector<bool>*, vector<bool>*> reverse_AN(Bat<Hin, Tin> *arg, typename Henc::type_t AH = Henc::A, typename Henc::type_t AHInv = Henc::A_INV, typename Henc::type_t AHUnencMaxU = Henc::A_UNENC_MAX_U, typename Tenc::type_t AT = Tenc::A, typename Tenc::type_t ATInv = Tenc::A_INV, typename Tenc::type_t ATUnencMaxU = Tenc::A_UNENC_MAX_U) {
                const bool isHeadEncoded = is_base_of<v2_anencoded_t, Hin>::value;
                const bool isTailEncoded = is_base_of<v2_anencoded_t, Tin>::value;
                size_t sizeBAT = arg->size();
                auto bat = new TempBat<Hout, Tout>(sizeBAT);
                vector<bool> *vecH = (isHeadEncoded ? new vector<bool> : nullptr);
                vector<bool> *vecT = (isTailEncoded ? new vector<bool> : nullptr);
                if (isHeadEncoded) vecH->reserve(sizeBAT);
                if (isTailEncoded) vecT->reserve(sizeBAT);
                auto iter = arg->begin();
                for (; iter->hasNext(); ++*iter) {
                    auto h = iter->head();
                    auto t = iter->tail();
                    if (isHeadEncoded) {
                        vecH->emplace_back((h * AHInv) <= AHUnencMaxU);
                    }
                    if (isTailEncoded) {
                        vecT->emplace_back((t * ATInv) <= ATUnencMaxU);
                    }
                    bat->append(make_pair((isTailEncoded ? t : (static_cast<typename Hout::type_t> (t) * AT)), (isHeadEncoded ? h : (static_cast<typename Tout::type_t> (h) * AH))));
                }
                delete iter;
                return make_tuple(bat, vecH, vecT);
            }

            template<typename Head1, typename Tail1, typename Head2, typename Tail2, typename H1Enc = typename TypeMap<Head1>::v2_encoded_t, typename T1Enc = typename TypeMap<Tail1>::v2_encoded_t, typename H2Enc = typename TypeMap<Head2>::v2_encoded_t, typename T2Enc = typename TypeMap<Tail2>::v2_encoded_t>
            tuple<Bat<Head1, Tail2>*, vector<bool>*, vector<bool>*, vector<bool>*, vector<bool>*> hashjoin_AN(Bat<Head1, Tail1>* arg1, Bat<Head2, Tail2>* arg2, typename H1Enc::type_t AH1 = H1Enc::A, typename H1Enc::type_t AH1inv = H1Enc::A_INV, typename H1Enc::type_t AH1UnencMaxU = H1Enc::A_UNENC_MAX_U, typename T1Enc::type_t AT1 = T1Enc::A, typename T1Enc::type_t AT1inv = T1Enc::A_INV, typename T1Enc::type_t AT1UnencMaxU = T1Enc::A_UNENC_MAX_U, typename H2Enc::type_t AH2 = H2Enc::A, typename H2Enc::type_t AH2inv = H2Enc::A_INV, typename H2Enc::type_t AH2UnencMaxU = H2Enc::A_UNENC_MAX_U, typename T2Enc::type_t AT2 = T2Enc::A, typename T2Enc::type_t AT2inv = T2Enc::A_INV, typename T2Enc::type_t AT2UnencMaxU = T2Enc::A_UNENC_MAX_U) {
                const bool isHead1Encoded = is_base_of<v2_anencoded_t, Head1>::value;
                const bool isTail1Encoded = is_base_of<v2_anencoded_t, Tail1>::value;
                const bool isHead2Encoded = is_base_of<v2_anencoded_t, Head2>::value;
                const bool isTail2Encoded = is_base_of<v2_anencoded_t, Tail2>::value;
                auto bat = new TempBat<Head1, Tail2>();
                vector<bool> *vec1 = (isHead1Encoded ? new vector<bool>() : nullptr);
                vector<bool> *vec2 = (isTail1Encoded ? new vector<bool>() : nullptr);
                vector<bool> *vec3 = (isHead2Encoded ? new vector<bool>() : nullptr);
                vector<bool> *vec4 = (isTail2Encoded ? new vector<bool>() : nullptr);
                auto iter1 = arg1->begin();
                auto iter2 = arg2->begin();
                if (iter1->hasNext() & iter2->hasNext()) {
                    // only really continue when both BATs are not empty
                    if (arg1->size() < arg2->size()) {
                        // let's ignore the joinSide for now and use that sizes as a measure, which is of course oversimplified
                        unordered_map<typename Tail1::type_t, vector<typename Head1::type_t> > hashMap;
                        for (; iter1->hasNext(); ++*iter1) { // build
                            auto h = iter1->head();
                            auto t = iter1->tail();
                            if (isHead1Encoded)
                                vec1->emplace_back((h * AH1inv) <= AH1UnencMaxU);
                            if (isTail1Encoded)
                                vec2->emplace_back((t * AT1inv) <= AT1UnencMaxU);
                            hashMap[t].emplace_back(h);
                        }
                        auto mapEnd = hashMap.end();
                        for (; iter2->hasNext(); ++*iter2) { // probe
                            auto h = iter2->head();
                            auto t = iter2->tail();
                            if (isHead2Encoded)
                                vec3->emplace_back((h * AH2inv) <= AH2UnencMaxU);
                            if (isTail2Encoded)
                                vec4->emplace_back((t * AT2inv) <= AT2UnencMaxU);
                            auto mapIter = hashMap.find(static_cast<typename Tail1::type_t> (isTail1Encoded ? (isHead2Encoded ? h : (static_cast<typename Tail1::type_t> (h) * AT1)) : (isHead2Encoded ? (h * AH2inv) : h)));
                            if (mapIter != mapEnd) {
                                for (auto matched : mapIter->second) {
                                    bat->append(make_pair(matched, t));
                                }
                            }
                        }
                    } else {
                        unordered_map<typename Head2::type_t, vector<typename Tail2::type_t> > hashMap;
                        for (; iter2->hasNext(); ++*iter2) { // build
                            auto h = iter2->head();
                            auto t = iter2->tail();
                            if (isHead2Encoded)
                                vec3->emplace_back((h * AH2inv) <= AH2UnencMaxU);
                            if (isTail2Encoded)
                                vec4->emplace_back((t * AT2inv) <= AT2UnencMaxU);
                            hashMap[h].emplace_back(t);
                        }
                        auto mapEnd = hashMap.end();
                        for (; iter1->hasNext(); ++*iter1) { // probe
                            auto h = iter1->head();
                            auto t = iter1->tail();
                            if (isHead1Encoded)
                                vec1->emplace_back((h * AH1inv) <= AH1UnencMaxU);
                            if (isTail1Encoded)
                                vec2->emplace_back((t * AT1inv) <= AT1UnencMaxU);
                            auto mapIter = hashMap.find(static_cast<typename Head2::type_t> (isHead2Encoded ? (isTail1Encoded ? t : (static_cast<typename Head2::type_t> (t) * AH2)) : (isTail1Encoded ? (t * AT1inv) : t)));
                            if (mapIter != mapEnd) {
                                for (auto matched : mapIter->second) {
                                    bat->append(make_pair(h, matched));
                                }
                            }
                        }
                    }
                }
                delete iter1;
                delete iter2;
                return make_tuple(bat, vec1, vec2, vec3, vec4);
            }

            /**
             * Multiplies the tail values of each of the two Bat's and sums everything up.
             * @param arg1
             * @param arg2
             * @return A single sum of the pair-wise products of the two Bats
             */
            template<typename Result, typename Head1, typename Tail1, typename Head2, typename Tail2, typename ResEnc = typename TypeMap<Result>::v2_encoded_t, typename T1Enc = typename TypeMap<Tail1>::v2_encoded_t, typename T2Enc = typename TypeMap<Tail2>::v2_encoded_t>
            tuple<Bat<v2_oid_t, Result>*, vector<bool>*, vector<bool>*> aggregate_mul_sum_AN(Bat<Head1, Tail1>* arg1, Bat<Head2, Tail2>* arg2, typename Result::type_t init = typename Result::type_t(0), typename T1Enc::type_t AT1 = T1Enc::A, typename T1Enc::type_t AT1inv = T1Enc::A_INV, typename T1Enc::type_t AT1unencMaxU = T1Enc::A_UNENC_MAX_U, typename T2Enc::type_t AT2 = T2Enc::A, typename T2Enc::type_t AT2inv = T2Enc::A_INV, typename T2Enc::type_t AT2unencMaxU = T2Enc::A_UNENC_MAX_U, typename ResEnc::type_t RA = ResEnc::A) {
                typedef typename Result::type_t result_t;
                const bool isTail1Encoded = is_base_of<v2_anencoded_t, Tail1>::value;
                const bool isTail2Encoded = is_base_of<v2_anencoded_t, Tail2>::value;
                const bool isResultEncoded = is_base_of<v2_anencoded_t, Result>::value;
                typename Result::type_t total = init;
                vector<bool>* vec1 = (isTail1Encoded ? new vector<bool>() : nullptr);
                vector<bool>* vec2 = (isTail2Encoded ? new vector<bool>() : nullptr);
                auto iter1 = arg1->begin();
                auto iter2 = arg2->begin();
                for (; iter1->hasNext() && iter2->hasNext(); ++*iter1, ++*iter2) {
                    typename T1Enc::type_t x1 = iter1->tail() * (isTail1Encoded ? AT1inv : 1);
                    typename T2Enc::type_t x2 = iter2->tail() * (isTail2Encoded ? AT2inv : 1);
                    if (isTail1Encoded)
                        vec1->emplace_back(x1 <= AT1unencMaxU);
                    if (isTail2Encoded)
                        vec2->emplace_back(x2 <= AT2unencMaxU);
                    total += static_cast<result_t> (x1) * static_cast<result_t> (x2);
                }
                if (isResultEncoded)
                    total *= RA;
                delete iter2;
                delete iter1;
                auto bat = new TempBat<v2_oid_t, Result>();
                bat->append(make_pair(0, total));
                return make_tuple(bat, vec1, vec2);
            }
        }
    }
}

#endif /* OPERATORSAN_TCC */
