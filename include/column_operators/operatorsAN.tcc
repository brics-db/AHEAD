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
            Bat<Head, typename TypeMap<Tail>::v2_encoded_t>* encode_AN(Bat<Head, Tail>* arg, typename TypeMap<Tail>::v2_encoded_t::type_t A = TypeMap<Tail>::v2_encoded_t::A) {
                typedef typename TypeMap<Tail>::v2_encoded_t::type_t tail_t;
                static_assert(is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                static_assert(is_base_of<v2_base_t, Tail>::value, "Tail must be a base type");
                auto result = new TempBat<Head, typename TypeMap<Tail>::v2_encoded_t > (arg->size());
                auto iter = arg->begin();
                for (; iter->hasNext(); ++*iter) {
                    result->append(make_pair(iter->head(), static_cast<tail_t> (iter->tail()) * A));
                }
                delete iter;
                return result; // possibly empty
            }

            template<typename Head, typename ResTail>
            vector<bool>* check_AN(Bat<Head, ResTail>* arg, typename ResTail::type_t aInv = ResTail::A_INV, typename ResTail::type_t unEncMaxU = ResTail::A_UNENC_MAX_U) {
                static_assert(is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                static_assert(is_base_of<v2_anencoded_t, ResTail>::value, "ResTail must be an AN-encoded type");
                auto result = new vector<bool>();
                result->reserve(arg->size());
                auto iter = arg->begin();
                for (; iter->hasNext(); ++*iter) {
                    result->emplace_back((iter->tail() * aInv) <= unEncMaxU);
                }
                delete iter;
                return result; // possibly empty
            }

            template<typename Head, typename ResTail>
            Bat<Head, typename ResTail::unenc_v2_t>* decode_AN(Bat<Head, ResTail>* arg, typename ResTail::type_t aInv = ResTail::A_INV, typename ResTail::type_t unEncMaxU = ResTail::A_UNENC_MAX_U) {
                static_assert(is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                static_assert(is_base_of<v2_anencoded_t, ResTail>::value, "ResTail must be an AN-encoded type");
                typedef typename ResTail::unenc_v2_t Tail;
                typedef typename Tail::type_t tail_t;
                auto result = new TempBat<Head, Tail>(arg->size());
                auto iter = arg->begin();
                for (; iter->hasNext(); ++*iter) {
                    result->append(make_pair(iter->head(), static_cast<tail_t> (iter->tail() * aInv)));
                }
                delete iter;
                return result;
            }

            template<typename Head, typename ResTail>
            pair<Bat<Head, typename ResTail::unenc_v2_t>*, vector<bool>*> checkAndDecode_AN(Bat<Head, ResTail>* arg, typename ResTail::type_t aInv = ResTail::A_INV, typename ResTail::type_t aUnencMaxU = ResTail::A_UNENC_MAX_U) {
                static_assert(is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                static_assert(is_base_of<v2_anencoded_t, ResTail>::value, "ResTail must be an AN-encoded type");
                size_t sizeBAT = arg->size();
                auto result = make_pair(new TempBat<Head, typename ResTail::unenc_v2_t > (sizeBAT), new vector<bool>());
                result.second->reserve(sizeBAT);
                auto iter = arg->begin();
                for (; iter->hasNext(); ++*iter) {
                    auto dec = iter->tail() * aInv;
                    result.first->append(make_pair(iter->head(), static_cast<typename ResTail::unenc_v2_t::type_t> (dec)));
                    result.second->emplace_back(move(dec <= aUnencMaxU));
                }
                delete iter;
                return result;
            }

            template<typename Op, typename Head, typename ResTail>
            struct SelectionAN1 {

                pair<Bat<typename Head::v2_select_t, typename ResTail::v2_select_t>*, vector<bool>*> operator()(Bat<Head, ResTail>* arg, typename ResTail::type_t threshold, typename ResTail::type_t aInv = ResTail::A_INV, typename ResTail::type_t unEncMaxU = ResTail::A_UNENC_MAX_U) {
                    static_assert(is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                    static_assert(is_base_of<v2_anencoded_t, ResTail>::value, "ResTail must be an AN-encoded type");
                    auto result = make_pair(new TempBat<typename Head::v2_select_t, typename ResTail::v2_select_t > (), new vector<bool>());
                    auto iter = arg->begin();
                    Op op;
                    for (; iter->hasNext(); ++*iter) {
                        auto t = iter->tail();
                        result.second->emplace_back((t * aInv) <= unEncMaxU);
                        if (op(t, threshold)) {
                            result.first->append(make_pair(iter->head(), t));
                        }
                    }
                    delete iter;
                    return result;
                }
            };

            template<typename Op1, typename Op2, typename Head, typename ResTail>
            struct SelectionAN2 {

                pair<Bat<typename Head::v2_select_t, typename ResTail::v2_select_t>*, vector<bool>*> operator()(Bat<Head, ResTail>* arg, typename ResTail::type_t threshold1, typename ResTail::type_t threshold2, typename ResTail::type_t aInv = ResTail::A_INV, typename ResTail::type_t unEncMaxU = ResTail::A_UNENC_MAX_U) {
                    static_assert(is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                    static_assert(is_base_of<v2_anencoded_t, ResTail>::value, "ResTail must be an AN-encoded type");
                    auto result = make_pair(new TempBat<typename Head::v2_select_t, typename ResTail::v2_select_t > (), new vector<bool>());
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

            template<template<typename> class Op, typename Head, typename ResTail>
            pair<Bat<typename Head::v2_select_t, typename ResTail::v2_select_t>*, vector<bool>*> selectAN(Bat<Head, ResTail>* arg, typename ResTail::type_t&& th1, typename ResTail::type_t aInv = ResTail::A_INV, typename ResTail::type_t unEncMaxU = ResTail::A_UNENC_MAX_U) {
                static_assert(is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                static_assert(is_base_of<v2_anencoded_t, ResTail>::value, "ResTail must be an AN-encoded type");
                SelectionAN1 < Op<typename ResTail::type_t>, Head, ResTail> impl;
                return impl(arg, move(th1));
            }

            template<template<typename> class Op1 = greater_equal, template<typename> class Op2 = less_equal, typename Head, typename ResTail>
            pair<Bat<typename Head::v2_select_t, typename ResTail::v2_select_t>*, vector<bool>*> selectAN(Bat<Head, ResTail>* arg, typename ResTail::type_t&& th1, typename ResTail::type_t&& th2, typename ResTail::type_t aInv = ResTail::A_INV, typename ResTail::type_t unEncMaxU = ResTail::A_UNENC_MAX_U) {
                static_assert(is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                static_assert(is_base_of<v2_anencoded_t, ResTail>::value, "ResTail must be an AN-encoded type");
                SelectionAN2 < Op1<typename ResTail::type_t>, Op2<typename ResTail::type_t>, Head, ResTail> impl;
                return impl(arg, move(th1), move(th2));
            }

            template<typename Head1, typename Tail1, typename Head2, typename Tail2, typename H1Enc = typename TypeMap<Head1>::v2_encoded_t, typename T1Enc = typename TypeMap<Tail1>::v2_encoded_t, typename H2Enc = typename TypeMap<Head2>::v2_encoded_t, typename T2Enc = typename TypeMap<Tail2>::v2_encoded_t>
            tuple<Bat<Head1, Tail2>*, vector<bool>*, vector<bool>*, vector<bool>*, vector<bool>*> hashjoinAN(Bat<Head1, Tail1>* arg1, Bat<Head2, Tail2>* arg2, typename H1Enc::type_t AH1 = H1Enc::A, typename H1Enc::type_t AH1inv = H1Enc::A_INV, typename H1Enc::type_t AH1UnencMaxU = H1Enc::A_UNENC_MAX_U, typename T1Enc::type_t AT1 = T1Enc::A, typename T1Enc::type_t AT1inv = T1Enc::A_INV, typename T1Enc::type_t AT1UnencMaxU = T1Enc::A_UNENC_MAX_U, typename H2Enc::type_t AH2 = H2Enc::A, typename H2Enc::type_t AH2inv = H2Enc::A_INV, typename H2Enc::type_t AH2UnencMaxU = H2Enc::A_UNENC_MAX_U, typename T2Enc::type_t AT2 = T2Enc::A, typename T2Enc::type_t AT2inv = T2Enc::A_INV, typename T2Enc::type_t AT2UnencMaxU = T2Enc::A_UNENC_MAX_U) {
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
            tuple<Bat<v2_oid_t, Result>*, vector<bool>*, vector<bool>*> aggregate_mul_sumAN(Bat<Head1, Tail1>* arg1, Bat<Head2, Tail2>* arg2, typename Result::type_t init = typename Result::type_t(0), typename T1Enc::type_t AT1 = T1Enc::A, typename T1Enc::type_t AT1inv = T1Enc::A_INV, typename T1Enc::type_t AT1unencMaxU = T1Enc::A_UNENC_MAX_U, typename T2Enc::type_t AT2 = T2Enc::A, typename T2Enc::type_t AT2inv = T2Enc::A_INV, typename T2Enc::type_t AT2unencMaxU = T2Enc::A_UNENC_MAX_U, typename ResEnc::type_t RA = ResEnc::A) {
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
