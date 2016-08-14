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

#include <ColumnStore.h>
#include <column_storage/Bat.h>
#include <column_storage/TempBat.h>
#include <util/resilience.hpp>

#include "operators.h"

namespace v2 {
    namespace bat {
        namespace ops {

            template<typename V2Type>
            typename TypeSelector<V2Type>::res_bat_t* encode_AN(typename TypeSelector<V2Type>::bat_t* arg, typename TypeSelector<V2Type>::res_t A = TypeSelector<V2Type>::A, size_t start = 0, size_t size = 0) {
                auto result = new typename TypeSelector<V2Type>::res_tmp_t(arg->size());
                auto iter = arg->begin();
                if (iter->hasNext()) {
                    auto next = iter->get(start);
                    result->append(std::move(make_pair(move(next.first), move(static_cast<typename TypeSelector<V2Type>::res_t> (next.second) * A))));
                    if (size) {
                        for (size_t step = 1; step < size && iter->hasNext(); ++step) {
                            next = iter->next();
                            result->append(move(make_pair(move(next.first), move(static_cast<typename TypeSelector<V2Type>::res_t> (next.second) * A))));
                        }
                    } else {
                        while (iter->hasNext()) {
                            next = iter->next();
                            result->append(move(make_pair(move(next.first), move(static_cast<typename TypeSelector<V2Type>::res_t> (next.second) * A))));
                        }
                    }
                }
                delete iter;
                return result; // possibly empty
            }

            template<typename V2Type>
            vector<bool>* check_AN(typename TypeSelector<V2Type>::res_bat_t* arg, typename TypeSelector<V2Type>::res_t aInv = TypeSelector<V2Type>::A_INV, typename TypeSelector<V2Type>::res_t unEncMaxU = TypeSelector<V2Type>::A_UNENC_MAX_U, size_t start = 0, size_t size = 0) {
                typedef typename TypeSelector<V2Type>::res_t Tail;
                auto result = new vector<bool>();
                result->reserve(arg->size());
                auto iter = arg->begin();
                if (iter->hasNext()) {
                    result->emplace_back(move((static_cast<Tail> (iter->get(start).second) * aInv) <= unEncMaxU));
                    if (size) {
                        for (size_t step = 1; step < size && iter->hasNext(); ++step) {
                            result->emplace_back(move((static_cast<Tail> (iter->next().second) * aInv) <= unEncMaxU));
                        }
                    } else {
                        while (iter->hasNext()) {
                            result->emplace_back(move((static_cast<Tail> (iter->next().second) * aInv) <= unEncMaxU));
                        }
                    }
                }
                delete iter;
                return result; // possibly empty
            }

            template<typename V2Type>
            typename TypeSelector<V2Type>::bat_t* decode_AN(typename TypeSelector<V2Type>::res_bat_t* arg, typename TypeSelector<V2Type>::res_t aInv = TypeSelector<V2Type>::A_INV, typename TypeSelector<V2Type>::res_t unEncMaxU = TypeSelector<V2Type>::A_UNENC_MAX_U, size_t start = 0, size_t size = 0) {
                typedef typename TypeSelector<V2Type>::base_t Tail;
                auto result = new typename TypeSelector<V2Type>::tmp_t(arg->size());
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

            template<typename V2Type>
            pair<typename TypeSelector<V2Type>::res_bat_t*, vector<bool>*> selection_lt_AN(typename TypeSelector<V2Type>::res_bat_t* arg, typename TypeSelector<V2Type>::res_t treshold, typename TypeSelector<V2Type>::res_t aInv = TypeSelector<V2Type>::A_INV, typename TypeSelector<V2Type>::res_t unEncMaxU = TypeSelector<V2Type>::A_UNENC_MAX_U) {
                size_t sizeBAT = arg->size();
                auto result = make_pair(new typename TypeSelector<V2Type>::res_tmp_t, new vector<bool>);
                result.second->reserve(sizeBAT);
                auto iter = arg->begin();
                while (iter->hasNext()) {
                    auto p = iter->next();
                    result.second->emplace_back((p.second * aInv) <= unEncMaxU);
                    if (p.second < treshold) {
                        result.first->append(move(make_pair(move(p.first), move(p.second))));
                    }
                }
                delete iter;
                return result;
            }

            template<typename V2Type>
            pair<typename TypeSelector<V2Type>::res_bat_t*, vector<bool>*> selection_bt_AN(typename TypeSelector<V2Type>::res_bat_t* arg, typename TypeSelector<V2Type>::res_t start, typename TypeSelector<V2Type>::res_t end, typename TypeSelector<V2Type>::res_t aInv = TypeSelector<V2Type>::A_INV, typename TypeSelector<V2Type>::res_t unEncMaxU = TypeSelector<V2Type>::A_UNENC_MAX_U) {
                size_t sizeBAT = arg->size();
                auto result = make_pair(new typename TypeSelector<V2Type>::res_tmp_t(), new vector<bool>);
                result.second->reserve(sizeBAT);
                auto iter = arg->begin();
                while (iter->hasNext()) {
                    auto p = iter->next();
                    result.second->emplace_back((p.second * aInv) <= unEncMaxU);
                    if (p.second <= end && p.second >= start) {
                        result.first->append(move(make_pair(move(p.first), move(p.second))));
                    }
                }
                delete iter;
                return result;
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

            template<typename V2Type2>
            tuple<typename TypeSelector<V2Type2>::res_bat_t*, vector<bool>*, vector<bool>*> col_hashjoin_AN(typename TypeSelector<v2_oid_t>::res_bat_t* arg1, typename TypeSelector<V2Type2>::res_bat_t* arg2, resoid_t A1 = TypeSelector<v2_oid_t>::A, resoid_t Ainv1 = TypeSelector<v2_oid_t>::A_INV, resoid_t maxUnEncU1 = TypeSelector<v2_oid_t>::A_UNENC_MAX_U, typename TypeSelector<V2Type2>::res_t A2 = TypeSelector<V2Type2>::A, typename TypeSelector<V2Type2>::res_t Ainv2 = TypeSelector<V2Type2>::A_INV, typename TypeSelector<V2Type2>::res_t maxUnEncU2 = TypeSelector<V2Type2>::A_UNENC_MAX_U, join_side_t joinSide = join_side_t::left) {
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
