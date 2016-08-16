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

#include <ColumnStore.h>
#include <column_storage/Bat.h>
#include <column_storage/TempBat.h>
#include <util/resilience.hpp>

#include "operators.h"

namespace v2 {
    namespace bat {
        namespace ops {

            template<typename Head, typename ResTail>
            Bat<Head, ResTail>* encode_AN(Bat<Head, typename ResTail::unenc_v2_t>* arg, typename ResTail::type_t A = ResTail::A, size_t start = 0, size_t size = 0) {
                auto result = new TempBat<Head, ResTail>(arg->size());
                auto iter = arg->begin();
                if (iter->hasNext()) {
                    auto next = iter->get(start);
                    result->append(std::move(make_pair(move(next.first), move(static_cast<typename ResTail::type_t> (next.second) * A))));
                    if (size) {
                        for (size_t step = 1; step < size && iter->hasNext(); ++step) {
                            next = iter->next();
                            result->append(move(make_pair(move(next.first), move(static_cast<typename ResTail::type_t> (next.second) * A))));
                        }
                    } else {
                        while (iter->hasNext()) {
                            next = iter->next();
                            result->append(move(make_pair(move(next.first), move(static_cast<typename ResTail::type_t> (next.second) * A))));
                        }
                    }
                }
                delete iter;
                return result; // possibly empty
            }

            template<typename Head, typename ResTail>
            vector<bool>* check_AN(Bat<Head, ResTail>* arg, typename ResTail::type_t aInv = ResTail::A_INV, typename ResTail::type_t unEncMaxU = ResTail::A_UNENC_MAX_U, size_t start = 0, size_t size = 0) {
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
            Bat<Head, typename ResTail::unenc_v2_t>* decode_AN(Bat<Head, ResTail>* arg, typename ResType::type_t aInv = ResType::A_INV, typename ResType::type_t unEncMaxU = ResType::A_UNENC_MAX_U, size_t start = 0, size_t size = 0) {
                typedef typename ResTail::unenc_v2_t Tail;
                auto result = new TempBat<Head, Tail>(arg->size());
                auto iter = arg->begin();
                if (iter->hasNext()) {
                    auto current = iter->get(start);
                    result->append(make_pair(move(current.first), move(static_cast<Tail> (current.second * aInv))));
                    if (size) {
                        for (size_t step = 1; step < size && iter->hasNext(); ++step) {
                            current = iter->next();
                            result->append(make_pair(move(current.first), move(static_cast<Tail> (current.second * aInv))));
                        }
                    } else {
                        while (iter->hasNext()) {
                            current = iter->next();
                            result->append(make_pair(move(current.first), move(static_cast<Tail> (current.second * aInv))));
                        }
                    }
                }
                delete iter;
                return result;
            }

            template<typename V2Type>
            pair<typename TypeSelector<V2Type>::bat_t*, vector<bool>*> checkAndDecode_AN(typename TypeSelector<V2Type>::res_bat_t* arg, typename TypeSelector<V2Type>::res_t aInv = TypeSelector<V2Type>::A_INV, typename TypeSelector<V2Type>::res_t unEncMaxU = TypeSelector<V2Type>::A_UNENC_MAX_U, size_t start = 0, size_t size = 0) {
                size_t sizeBAT = arg->size();
                auto result = make_pair(new typename TypeSelector<V2Type>::tmp_t(sizeBAT), new vector<bool>());
                result.second->reserve(sizeBAT);
                auto iter = arg->begin();
                if (iter->hasNext()) {
                    auto current = iter->get(start);
                    typename TypeSelector<V2Type>::res_t dec = current.second * aInv;
                    result.first->append(move(make_pair(move(current.first), move(static_cast<typename TypeSelector<V2Type>::base_t> (dec)))));
                    result.second->emplace_back(move(dec <= unEncMaxU));
                    if (size) {
                        for (size_t step = 1; step < size && iter->hasNext(); ++step) {
                            current = iter->next();
                            dec = current.second * aInv;
                            result.first->append(move(make_pair(move(current.first), move(static_cast<typename TypeSelector<V2Type>::base_t> (dec)))));
                            result.second->emplace_back(move(dec <= unEncMaxU));
                        }
                    } else {
                        while (iter->hasNext()) {
                            current = iter->next();
                            dec = current.second * aInv;
                            result.first->append(move(make_pair(move(current.first), move(static_cast<typename TypeSelector<V2Type>::base_t> (dec)))));
                            result.second->emplace_back(move(dec <= unEncMaxU));
                        }
                    }
                }
                delete iter;
                return result;
            }

            template<typename V2Type, typename Op>
            pair<typename TypeSelector<V2Type>::res_bat_t*, vector<bool>*> selection_AN(typename TypeSelector<V2Type>::res_bat_t* arg, typename TypeSelector<V2Type>::res_t threshold, typename TypeSelector<V2Type>::res_t aInv = TypeSelector<V2Type>::A_INV, typename TypeSelector<V2Type>::res_t unEncMaxU = TypeSelector<V2Type>::A_UNENC_MAX_U) {
                size_t sizeBAT = arg->size();
                auto result = make_pair(new typename TypeSelector<V2Type>::res_tmp_t, new vector<bool>);
                result.second->reserve(sizeBAT);
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

            template<typename V2Type, typename Op1, typename Op2>
            pair<typename TypeSelector<V2Type>::res_bat_t*, vector<bool>*> selection_AN(typename TypeSelector<V2Type>::res_bat_t* arg, typename TypeSelector<V2Type>::res_t threshold1, typename TypeSelector<V2Type>::res_t threshold2, typename TypeSelector<V2Type>::res_t aInv = TypeSelector<V2Type>::A_INV, typename TypeSelector<V2Type>::res_t unEncMaxU = TypeSelector<V2Type>::A_UNENC_MAX_U) {
                size_t sizeBAT = arg->size();
                auto result = make_pair(new typename TypeSelector<V2Type>::res_tmp_t, new vector<bool>);
                result.second->reserve(sizeBAT);
                auto iter = arg->begin();
                while (iter->hasNext()) {
                    auto p = iter->next();
                    result.second->emplace_back((p.second * aInv) <= unEncMaxU);
                    if (Op1(p.second, threshold1) && Op2(p.second, threshold2)) {
                        result.first->append(move(make_pair(move(p.first), move(p.second))));
                    }
                }
                delete iter;
                return result;
            }

            template<typename V2Type>
            pair<typename TypeSelector<V2Type>::res_bat_t*, vector<bool>*> selection_AN(selection_type_t selType, typename TypeSelector<V2Type>::res_bat_t* arg, typename TypeSelector<V2Type>::res_t threshold1, typename TypeSelector<V2Type>::res_t threshold2 = typename TypeSelector<V2Type>::res_t(0), typename TypeSelector<V2Type>::res_t aInv = TypeSelector<V2Type>::A_INV, typename TypeSelector<V2Type>::res_t unEncMaxU = TypeSelector<V2Type>::A_UNENC_MAX_U) {
                switch (selType) {
                    case selection_type_t::LT:
                        return selection_AN<V2Type, std::less<typename TypeSelector<V2Type>::res_t >> (arg, threshold1);
                    case selection_type_t::LE:
                        return selection_AN<V2Type, std::less_equal<typename TypeSelector<V2Type>::res_t >> (arg, threshold1);
                    case selection_type_t::EQ:
                        return selection_AN<V2Type, std::equal_to>(arg, threshold1);
                    case selection_type_t::GE:
                        return selection_AN<V2Type, std::equal_to>(arg, threshold1);
                    case selection_type_t::GT:
                        return selection_AN<V2Type, std::equal_to>(arg, threshold1);
                    case selection_type_t::BT:
                        return selection_AN<V2Type, std::equal_to>(arg, threshold1, threshold2);
                    default:
                        stringstream ss;
                        ss << "Unknown selection type \"" << selType << '"';
                        throw runtime_error(ss.str());
                }
            }

            template<typename V2Type>
            typename TypeSelector<v2_oid_t>::res_bat_t* mirrorHead_AN(typename TypeSelector<V2Type>::res_bat_t* arg, typename TypeSelector<v2_oid_t>::res_t A = TypeSelector<v2_oid_t>::A) {
                auto result = new typename TypeSelector<v2_oid_t>::res_tmp_t(arg->size());
                auto iter = arg->begin();
                while (iter->hasNext()) {
                    auto p = iter->next();
                    result->append(make_pair(p.first, static_cast<resoid_t> (p.first) * A));
                }
                delete iter;
                return result;
            }

            tuple<typename TypeSelector<v2_oid_t>::res_bat_rev_t*, vector<bool>*, vector<bool>*> mirrorHead_resoid_AN(typename TypeSelector<v2_oid_t>::res_bat_rev_t* arg, resoid_t aInv = TypeSelector<v2_oid_t>::A_INV, resoid_t aUnencMaxU = TypeSelector<v2_oid_t>::A_UNENC_MAX_U) {
                auto bat = new typename TypeSelector<v2_oid_t>::res_tmp_rev_t(arg->size());
                auto vec1 = new vector<bool>, vec2 = new vector<bool>;
                auto iter = arg->begin();
                while (iter->hasNext()) {
                    auto p = iter->next();
                    vec1->emplace_back((p.first * aInv) <= aUnencMaxU);
                    vec2->emplace_back((p.second * aInv) <= aUnencMaxU);
                    bat->append(make_pair(p.first, p.first));
                }
                delete iter;
                return make_tuple(bat, vec1, vec2);
            }

            template <class V2Type>
            pair<typename TypeSelector<V2Type>::res_tmp_rev_t*, vector<bool>*> reverse_AN(typename TypeSelector<V2Type>::res_bat_t *arg, typename TypeSelector<V2Type>::res_t aInv = TypeSelector<V2Type>::A, typename TypeSelector<V2Type>::res_t unEncMaxU = TypeSelector<V2Type>::A_UNENC_MAX_U, resoid_t Aoid = TypeSelector<v2_oid_t>::A) {
                size_t sizeBAT = arg->size();
                auto result = make_pair(new typename TypeSelector<V2Type>::res_tmp_rev_t(sizeBAT), new vector<bool>);
                result.second->reserve(sizeBAT);
                auto iter = arg->begin();
                while (iter->hasNext()) {
                    auto p = iter->next();
                    result.second->emplace_back((p.second * aInv) <= unEncMaxU);
                    result.first->append(make_pair(p.second, static_cast<resoid_t> (p.first) * Aoid));
                }
                delete iter;
                return result;
            }

            template<typename V2Type1, typename V2Type2>
            tuple<typename TypeSelector<v2_oid_t>::res_bat_rev_t*, vector<bool>*, vector<bool>*> col_hashjoin_AN(typename TypeSelector<V2Type1>::res_bat_t* arg1, typename TypeSelector<V2Type2>::res_bat_rev_t* arg2, join_side_t joinSide = join_side_t::left, resoid_t A1 = TypeSelector<v2_oid_t>::A, resoid_t Ainv1 = TypeSelector<v2_oid_t>::A_INV, resoid_t maxUnEncU1 = TypeSelector<v2_oid_t>::A_UNENC_MAX_U, typename TypeSelector<V2Type2>::res_t A2 = TypeSelector<V2Type2>::A, typename TypeSelector<V2Type2>::res_t Ainv2 = TypeSelector<V2Type2>::A_INV, typename TypeSelector<V2Type2>::res_t maxUnEncU2 = TypeSelector<V2Type2>::A_UNENC_MAX_U, resoid_t AoidInv = TypeSelector<v2_oid_t>::A) {
                auto bat = new typename TypeSelector<v2_oid_t>::res_tmp_rev_t;
                auto v1 = new vector<bool>, v2 = new vector<bool>;
                auto iter1 = arg1->begin();
                auto iter2 = arg2->begin();
                // TODO implement
                delete iter1;
                delete iter2;
                return make_tuple(move(bat), move(v1), move(v2));
            }

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
        }
    }
}

#endif /* OPERATORSAN_TCC */
