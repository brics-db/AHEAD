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
                    auto next = iter->get(start);
                    result->append(std::move(make_pair(move(next.first), move(static_cast<tail_t> (next.second) * A))));
                    if (size) {
                        for (size_t step = 1; step < size && iter->hasNext(); ++step) {
                            next = iter->next();
                            result->append(move(make_pair(move(next.first), move(static_cast<tail_t> (next.second) * A))));
                        }
                    } else {
                        while (iter->hasNext()) {
                            next = iter->next();
                            result->append(move(make_pair(move(next.first), move(static_cast<tail_t> (next.second) * A))));
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
                    result->emplace_back(((iter->get(start).second) * aInv) <= unEncMaxU);
                    if (size) {
                        for (size_t step = 1; step < size && iter->hasNext(); ++step) {
                            result->emplace_back(move((iter->next().second * aInv) <= unEncMaxU));
                        }
                    } else {
                        while (iter->hasNext()) {
                            result->emplace_back(move((iter->next().second * aInv) <= unEncMaxU));
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
                    auto current = iter->get(start);
                    result->append(make_pair(move(current.first), move(static_cast<tail_t> (current.second * aInv))));
                    if (size) {
                        for (size_t step = 1; step < size && iter->hasNext(); ++step) {
                            current = iter->next();
                            result->append(make_pair(move(current.first), move(static_cast<tail_t> (current.second * aInv))));
                        }
                    } else {
                        while (iter->hasNext()) {
                            current = iter->next();
                            result->append(make_pair(move(current.first), move(static_cast<tail_t> (current.second * aInv))));
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
                    auto current = iter->get(start);
                    typename ResTail::type_t dec = current.second * aInv;
                    result.first->append(move(make_pair(move(current.first), move(static_cast<typename ResTail::unenc_v2_t::type_t> (dec)))));
                    result.second->emplace_back(move(dec <= aUnencMaxU));
                    if (size) {
                        for (size_t step = 1; step < size && iter->hasNext(); ++step) {
                            current = iter->next();
                            dec = current.second * aInv;
                            result.first->append(move(make_pair(move(current.first), move(static_cast<typename ResTail::unenc_v2_t::type_t> (dec)))));
                            result.second->emplace_back(move(dec <= aUnencMaxU));
                        }
                    } else {
                        while (iter->hasNext()) {
                            current = iter->next();
                            dec = current.second * aInv;
                            result.first->append(move(make_pair(move(current.first), move(static_cast<typename ResTail::unenc_v2_t::type_t> (dec)))));
                            result.second->emplace_back(move(dec <= aUnencMaxU));
                        }
                    }
                }
                delete iter;
                return result;
            }

            template<typename Head, typename ResTail, typename Op>
            pair<Bat<Head, ResTail>*, vector<bool>*> selection_AN(Bat<Head, ResTail>* arg, typename ResTail::type_t threshold, typename ResTail::type_t aInv = ResTail::A_INV, typename ResTail::type_t unEncMaxU = ResTail::A_UNENC_MAX_U) {
                static_assert(is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                static_assert(is_base_of<v2_anencoded_t, ResTail>::value, "ResTail must be an AN-encoded type");
                auto result = make_pair(new TempBat<Head, ResTail>(), new vector<bool>());
                auto iter = arg->begin();
                Op op;
                while (iter->hasNext()) {
                    auto p = iter->next();
                    result.second->emplace_back((p.second * aInv) <= unEncMaxU);
                    if (op(p.second, threshold)) {
                        result.first->append(move(make_pair(move(p.first), move(p.second))));
                    }
                }
                delete iter;
                return result;
            }

            template<typename Head, typename ResTail, typename Op1, typename Op2>
            pair<Bat<Head, ResTail>*, vector<bool>*> selection_AN(Bat<Head, ResTail>* arg, typename ResTail::type_t threshold1, typename ResTail::type_t threshold2, typename ResTail::type_t aInv = ResTail::A_INV, typename ResTail::type_t unEncMaxU = ResTail::A_UNENC_MAX_U) {
                static_assert(is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                static_assert(is_base_of<v2_anencoded_t, ResTail>::value, "ResTail must be an AN-encoded type");
                auto result = make_pair(new TempBat<Head, ResTail>(), new vector<bool>());
                auto iter = arg->begin();
                Op1 op1;
                Op2 op2;
                while (iter->hasNext()) {
                    auto p = iter->next();
                    result.second->emplace_back((p.second * aInv) <= unEncMaxU);
                    if (op1(p.second, threshold1) && op2(p.second, threshold2)) {
                        result.first->append(move(make_pair(move(p.first), move(p.second))));
                    }
                }
                delete iter;
                return result;
            }

            template<typename Head, typename ResTail>
            pair<Bat<Head, ResTail>*, vector<bool>*> selection_AN(selection_type_t selType, Bat<Head, ResTail>* arg, typename ResTail::type_t threshold1, typename ResTail::type_t threshold2 = typename ResTail::type_t(0), typename ResTail::type_t aInv = ResTail::A_INV, typename ResTail::type_t unEncMaxU = ResTail::A_UNENC_MAX_U) {
                static_assert(is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                static_assert(is_base_of<v2_anencoded_t, ResTail>::value, "ResTail must be an AN-encoded type");
                switch (selType) {
                    case selection_type_t::LT:
                        return selection_AN<Head, ResTail, std::less<typename ResTail::type_t >> (arg, threshold1);
                    case selection_type_t::LE:
                        return selection_AN<Head, ResTail, std::less_equal<typename ResTail::type_t >> (arg, threshold1);
                    case selection_type_t::EQ:
                        return selection_AN<Head, ResTail, std::equal_to<typename ResTail::type_t >> (arg, threshold1);
                    case selection_type_t::GE:
                        return selection_AN<Head, ResTail, std::equal_to<typename ResTail::type_t >> (arg, threshold1);
                    case selection_type_t::GT:
                        return selection_AN<Head, ResTail, std::equal_to<typename ResTail::type_t >> (arg, threshold1);
                    case selection_type_t::BT:
                        return selection_AN<Head, ResTail, std::greater_equal<typename ResTail::type_t >, std::less_equal<typename ResTail::type_t >> (arg, threshold1, threshold2);
                    default:
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
                    while (iter->hasNext()) {
                        auto p = iter->next();
                        result.second->emplace_back((p.first * aInv) < aUnencMaxU);
                        result.first->append(make_pair(p.first, p.first));
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
                    while (iter->hasNext()) {
                        auto p = iter->next();
                        bat->append(make_pair(p.first, static_cast<typename ResHead::type_t> (p.first) * A));
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
                while (iter->hasNext()) {
                    auto p = iter->next();
                    result->append(move(make_pair(move(static_cast<typename ResTail::type_t> (p.second) * Atail), move(static_cast<typename ResHead::type_t> (p.first) * Ahead))));
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
                while (iter->hasNext()) {
                    auto p = iter->next();
                    if (isHeadEncoded) {
                        vecH->emplace_back((p.first * AHInv) <= AHUnencMaxU);
                    }
                    if (isTailEncoded) {
                        vecT->emplace_back((p.second * ATInv) <= ATUnencMaxU);
                    }
                    bat->append(make_pair((isTailEncoded ? p.second : (static_cast<typename Hout::type_t> (p.second) * AT)), (isHeadEncoded ? p.first : (static_cast<typename Tout::type_t> (p.first) * AH))));
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
                if (iter1->hasNext() & iter2->hasNext()) { // only really continue when both BATs are not empty
                    if (arg1->size() < arg2->size()) { // let's ignore the joinSide for now and use that sizes as a measure, which is of course oversimplified
                        unordered_map<typename Tail1::type_t, vector<typename Head1::type_t> > hashMap;
                        while (iter1->hasNext()) { // build
                            auto pairLeft = iter1->next();
                            if (isHead1Encoded)
                                vec1->emplace_back((pairLeft.first * AH1inv) <= AH1UnencMaxU);
                            if (isTail1Encoded)
                                vec2->emplace_back((pairLeft.second * AT1inv) <= AT1UnencMaxU);
                            hashMap[pairLeft.second].emplace_back(pairLeft.first);
                        }
                        auto mapEnd = hashMap.end();
                        while (iter2->hasNext()) { // probe
                            auto pairRight = iter2->next();
                            if (isHead2Encoded)
                                vec3->emplace_back((pairRight.first * AH2inv) <= AH2UnencMaxU);
                            if (isTail2Encoded)
                                vec4->emplace_back((pairRight.second * AT2inv) <= AT2UnencMaxU);
                            auto mapIter = hashMap.find(static_cast<typename Tail1::type_t> (isTail1Encoded ? (isHead2Encoded ? pairRight.first : (static_cast<typename Tail1::type_t> (pairRight.first) * AT1)) : (isHead2Encoded ? (pairRight.first * AH2inv) : pairRight.first)));
                            if (mapIter != mapEnd) {
                                for (auto matched : mapIter->second) {
                                    bat->append(make_pair(matched, pairRight.second));
                                }
                            }
                        }
                    } else {
                        unordered_map<typename Head2::type_t, vector<typename Tail2::type_t> > hashMap;
                        while (iter2->hasNext()) { // build
                            auto pairRight = iter2->next();
                            if (isHead2Encoded)
                                vec3->emplace_back((pairRight.first * AH2inv) <= AH2UnencMaxU);
                            if (isTail2Encoded)
                                vec4->emplace_back((pairRight.second * AT2inv) <= AT2UnencMaxU);
                            hashMap[pairRight.first].emplace_back(pairRight.second);
                        }
                        auto mapEnd = hashMap.end();
                        while (iter1->hasNext()) { // probe
                            auto pairLeft = iter1->next();
                            if (isHead1Encoded)
                                vec1->emplace_back((pairLeft.first * AH1inv) <= AH1UnencMaxU);
                            if (isTail1Encoded)
                                vec2->emplace_back((pairLeft.second * AT1inv) <= AT1UnencMaxU);
                            auto mapIter = hashMap.find(static_cast<typename Head2::type_t> (isHead2Encoded ? (isTail1Encoded ? pairLeft.second : (static_cast<typename Head2::type_t> (pairLeft.second) * AH2)) : (isTail1Encoded ? (pairLeft.second * AT1inv) : pairLeft.second)));
                            if (mapIter != mapEnd) {
                                for (auto matched : mapIter->second) {
                                    bat->append(make_pair(pairLeft.first, matched));
                                }
                            }
                        }
                    }
                }
                delete iter1;
                delete iter2;
                return make_tuple(bat, vec1, vec2, vec3, vec4);
            }

            /*
            template<typename V2Type2>
            tuple<typename TypeSelector<V2Type2>::res_bat_t*, vector<bool>*, vector<bool>*> col_hashjoin_AN(typename TypeSelector<v2_oid_t>::res_bat_t* arg1, typename TypeSelector<V2Type2>::res_bat_t* arg2, join_side_t joinSide = join_side_t::left, resoid_t A1 = TypeSelector<v2_oid_t>::A, resoid_t Ainv1 = TypeSelector<v2_oid_t>::A_INV, resoid_t maxUnEncU1 = TypeSelector<v2_oid_t>::A_UNENC_MAX_U, typename TypeSelector<V2Type2>::res_t A2 = TypeSelector<V2Type2>::A, typename TypeSelector<V2Type2>::res_t Ainv2 = TypeSelector<V2Type2>::A_INV, typename TypeSelector<V2Type2>::res_t maxUnEncU2 = TypeSelector<V2Type2>::A_UNENC_MAX_U) {
                auto bat = new typename TypeSelector<V2Type2>::res_tmp_t;
                auto v1 = new vector<bool>, v2 = new vector<bool>;
                auto iter1 = arg1->begin();
                auto iter2 = arg2->begin();
                if (iter1->hasNext() & iter2->hasNext()) { // only really continue when both BATs are not empty
                    if (arg1->size() < arg2->size()) { // let's ignore the joinSide for now and use that sizes as a measure
                        unordered_map<resoid_t, vector<oid_t> > hashMap;
                        while (iter1->hasNext()) { // build
                            auto pairLeft = iter1->next();
                            v1->emplace_back(static_cast<typename TypeSelector<v2_oid_t>::res_t> (pairLeft.second * Ainv1) <= maxUnEncU1);
                            hashMap[pairLeft.second].emplace_back(pairLeft.first);
                        }
                        auto mapEnd = hashMap.end();
                        while (iter2->hasNext()) { // probe
                            auto pairRight = iter2->next();
                            v2->emplace_back(static_cast<typename TypeSelector<V2Type2>::res_t> (pairRight.second * Ainv2) <= maxUnEncU2);
                            auto mapIter = hashMap.find(static_cast<resoid_t> (pairRight.first) * A1);
                            if (mapIter != mapEnd) {
                                for (auto matched : mapIter->second) {
                                    bat->append(make_pair(matched, pairRight.second));
                                }
                            }
                        }
                    } else {
                        unordered_map<resoid_t, vector<typename TypeSelector<V2Type2>::res_t> > hashMap;
                        while (iter2->hasNext()) {
                            auto pairRight = iter2->next();
                            v2->emplace_back(static_cast<typename TypeSelector<V2Type2>::res_t> (pairRight.second * Ainv2) <= maxUnEncU2);
                            hashMap[static_cast<resoid_t> (pairRight.first) * A1].emplace_back(pairRight.second);
                        }
                        auto mapEnd = hashMap.end();
                        while (iter1->hasNext()) { // probe
                            auto pairLeft = iter1->next();
                            v1->emplace_back(static_cast<typename TypeSelector<v2_oid_t>::res_t> (pairLeft.second * Ainv1) <= maxUnEncU1);
                            auto mapIter = hashMap.find(pairLeft.second);
                            if (mapIter != mapEnd) {
                                for (auto matched : mapIter->second) {
                                    bat->append(make_pair(pairLeft.first, matched));
                                }
                            }
                        }
                    }
                }
                delete iter1;
                delete iter2;
                return make_tuple(move(bat), move(v1), move(v2));
            }
             */
        }
    }
}

#endif /* OPERATORSAN_TCC */
