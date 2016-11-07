// Copyright (c) 2010 Dirk Habich
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


/***
 * @author Dirk Habich
 * @author Till Kolditz
 */
#ifndef OPERATORS_H
#define OPERATORS_H

#include <vector>
#include <iostream>
#include <map>
#include <unordered_map>
#include <cmath>

#include <ColumnStore.h>
#include <column_storage/Bat.h>
#include <column_storage/TempBat.h>

typedef enum {
    EQ, LT, LE, GT, GE, BT
} selection_type_t;

typedef enum {
    left, right
} hash_side_t;

namespace v2 {
    namespace bat {
        namespace ops {

            template<typename Head, typename Tail>
            Bat<typename Head::v2_copy_t, typename Tail::v2_copy_t>* copy(Bat<Head, Tail>* arg) {
                auto result = new TempBat<Head, Tail>(arg->size());
                auto *iter = arg->begin();
                for (; iter->hasNext(); ++*iter) {
                    result->append(make_pair(iter->head(), iter->tail()));
                }
                delete iter;
                return result;
            }

            template<typename Op, typename Head, typename Tail, typename TH>
            struct Selection1 {

                Bat<typename Head::v2_select_t, typename Tail::v2_select_t>* operator()(Bat<Head, Tail>* arg, TH&& threshold) {
                    auto result = new TempBat<typename Head::v2_select_t, typename Tail::v2_select_t > ();
                    auto iter = arg->begin();
                    Op op;
                    for (; iter->hasNext(); ++*iter) {
                        auto t = iter->tail();
                        if (op(t, threshold)) {
                            result->append(make_pair(iter->head(), t));
                        }
                    }
                    delete iter;
                    return result;
                }
            };

            template<typename Op, typename Head, typename TH>
            struct Selection1<Op, Head, v2_str_t, TH> {

                Bat<typename Head::v2_select_t, typename v2_str_t::v2_select_t>* operator()(Bat<Head, v2_str_t>* arg, TH&& threshold) {
                    auto result = new TempBat<typename Head::v2_select_t, typename v2_str_t::v2_select_t > ();
                    auto iter = arg->begin();
                    Op op;
                    for (; iter->hasNext(); ++*iter) {
                        auto t = iter->tail();
                        if (op(strcmp(t, threshold), 0)) {
                            result->append(make_pair(iter->head(), t));
                        }
                    }
                    return result;
                }
            };

            template<typename Op, typename Head, typename TH>
            struct Selection1<Op, Head, v2_cstr_t, TH> {

                Bat<typename Head::v2_select_t, typename v2_cstr_t::v2_select_t>* operator()(Bat<Head, v2_cstr_t>* arg, TH&& threshold) {
                    auto result = new TempBat<typename Head::v2_select_t, typename v2_cstr_t::v2_select_t > ();
                    auto iter = arg->begin();
                    Op op;
                    for (; iter->hasNext(); ++*iter) {
                        auto t = iter->tail();
                        if (op(strcmp(t, threshold), 0)) {
                            result->append(make_pair(iter->head(), t));
                        }
                    }
                    return result;
                }
            };

            template<typename Op1, typename Op2, typename Head, typename Tail, typename TH>
            struct Selection2 {

                Bat<typename Head::v2_select_t, typename Tail::v2_select_t>* operator()(Bat<Head, Tail>* arg, TH&& th1, TH&& th2) {
                    auto result = new TempBat<typename Head::v2_select_t, typename Tail::v2_select_t > ();
                    auto iter = arg->begin();
                    Op1 op1;
                    Op2 op2;
                    for (; iter->hasNext(); ++*iter) {
                        auto t = iter->tail();
                        if (op1(t, th1) && op2(t, th2)) {
                            result->append(make_pair(iter->head(), t));
                        }
                    }
                    delete iter;
                    return result;
                }
            };

            template<typename Op1, typename Op2, typename Head, typename TH>
            struct Selection2<Op1, Op2, Head, v2_str_t, TH> {

                Bat<typename Head::v2_select_t, typename v2_str_t::v2_select_t>* operator()(Bat<Head, v2_str_t>* arg, TH&& th1, TH&& th2) {
                    auto result = new TempBat<typename Head::v2_select_t, typename v2_str_t::v2_select_t > ();
                    auto iter = arg->begin();
                    Op1 op1;
                    Op2 op2;
                    for (; iter->hasNext(); ++*iter) {
                        auto t = iter->tail();
                        if (op1(strcmp(t, th1), 0) && op2(strcmp(t, th2), 0)) {
                            result->append(make_pair(iter->head(), t));
                        }
                    }
                    delete iter;
                    return result;
                }
            };

            template<typename Op1, typename Op2, typename Head, typename TH>
            struct Selection2<Op1, Op2, Head, v2_cstr_t, TH> {

                Bat<typename Head::v2_select_t, typename v2_cstr_t::v2_select_t>* operator()(Bat<Head, v2_cstr_t>* arg, TH&& th1, TH&& th2) {
                    auto result = new TempBat<typename Head::v2_select_t, typename v2_cstr_t::v2_select_t > ();
                    auto iter = arg->begin();
                    Op1 op1;
                    Op2 op2;
                    for (; iter->hasNext(); ++*iter) {
                        auto t = iter->tail();
                        if (op1(strcmp(t, th1), 0) && op2(strcmp(t, th2), 0)) {
                            result->append(make_pair(iter->head(), t));
                        }
                    }
                    delete iter;
                    return result;
                }
            };

            template<template <typename> class Op, typename Head, typename Tail, typename TH>
            Bat<typename Head::v2_select_t, typename Tail::v2_select_t>* select(Bat<Head, Tail>* arg, TH&& th1) {
                Selection1 < Op<typename Tail::v2_compare_t::type_t>, Head, Tail, TH> impl;
                return impl(arg, move(th1));
            }

            template<template<typename> class Op1 = greater_equal, template<typename> class Op2 = less_equal, typename Head, typename Tail, typename TH>
            Bat<typename Head::v2_select_t, typename Tail::v2_select_t>* select(Bat<Head, Tail>* arg, TH&& th1, TH&& th2) {
                Selection2 < Op1<typename Tail::type_t>, Op2<typename Tail::type_t>, Head, Tail, TH> impl;
                return impl(arg, move(th1), move(th2));
            }

            template<typename T1, typename T2, typename T3, typename T4>
            Bat<typename T1::v2_select_t, typename T4::v2_select_t>* hashjoin(Bat<T1, T2> *arg1, Bat<T3, T4> *arg2, hash_side_t side = hash_side_t::left) {
                auto result = new TempBat<typename T1::v2_select_t, typename T4::v2_select_t > ();
                auto iter1 = arg1->begin();
                auto iter2 = arg2->begin();
                if (iter1->hasNext() && iter2->hasNext()) {
                    if (side == hash_side_t::left) {
                        unordered_map<typename T2::type_t, vector<typename T1::type_t> > hashMap;
                        for (; iter1->hasNext(); ++*iter1) {
                            hashMap[iter1->tail()].emplace_back(move(iter1->head()));
                        }
                        auto mapEnd = hashMap.end();
                        for (; iter2->hasNext(); ++*iter2) {
                            auto iterMap = hashMap.find(iter2->head());
                            if (iterMap != mapEnd) {
                                auto t2 = iter2->tail();
                                for (auto matched : iterMap->second) {
                                    result->append(make_pair(move(matched), move(t2)));
                                }
                            }
                        }
                    } else {
                        unordered_map<typename T3::type_t, vector<typename T4::type_t> > hashMap;
                        for (; iter2->hasNext(); ++*iter2) {
                            hashMap[iter2->head()].emplace_back(iter2->tail());
                        }
                        auto mapEnd = hashMap.end();
                        for (; iter1->hasNext(); ++*iter1) {
                            auto iterMap = hashMap.find(iter1->tail());
                            if (iterMap != mapEnd) {
                                auto h1 = iter1->head();
                                for (auto matched : iterMap->second) {
                                    result->append(make_pair(move(h1), move(matched)));
                                }
                            }
                        }
                    }
                }
                delete iter1;
                delete iter2;
                return result;
            }

            /**
             * Simple sum of all tail values in a Bat
             * @param arg a Bat
             * @return a single sum value
             */
            template<typename Head, typename Tail>
            Tail aggregate_sum(Bat<Head, Tail>* arg) {
                Tail sum = 0;
                auto iter = arg->begin();
                for (; iter->hasNext(); ++*iter) {
                    sum += iter->tail();
                }
                delete iter;
                return sum;
            }

            /**
             * Multiplies the tail values of each of the two Bat's and sums everything up.
             * @param arg1
             * @param arg2
             * @return A single sum of the pair-wise products of the two Bats
             */
            template<typename Result, typename Head1, typename Tail1, typename Head2, typename Tail2>
            Result aggregate_mul_sum(Bat<Head1, Tail1>* arg1, Bat<Head2, Tail2>* arg2, Result init = Result(0)) {
                auto iter1 = arg1->begin();
                auto iter2 = arg2->begin();
                Result total = init;
                for (; iter1->hasNext() && iter2->hasNext(); ++*iter1, ++*iter2) {
                    total += (static_cast<Result> (iter1->tail()) * static_cast<Result> (iter2->tail()));
                }
                delete iter2;
                delete iter1;
                return total;
            }

            /**
             * Group-By the tail
             * Returns 2 BAT's:
             * 1) Mapping (V)OID -> GroupID
             * 2) GroupID -> Value
             */
            template<typename Head, typename Tail>
            struct groupby {

                pair<Bat<Head, v2_oid_t>*, Bat<v2_void_t, Tail>*> group(Bat<Head, Tail>* bat) {
                    auto mapHeadtoGID = new TempBat<Head, v2_oid_t>();
                    auto mapGIDtoTail = new TempBat<v2_void_t, Tail>();
                    auto iter = bat->begin();
                    for (; iter->hasNext(); ++*iter) {
                        typename Tail::type_t curTail = iter->tail();
                        // search this tail in our mapping
                        // idx is the void value of mapGIDtoTail, which starts at zero
                        size_t curGID = 0;
                        bool isNewGroup = true;
                        for (auto itertail = mapGIDtoTail->tail.container->begin(); itertail != mapGIDtoTail->tail.container->end(); ++itertail) {
                            if (curTail == *itertail) {
                                isNewGroup = false;
                                break;
                            }
                            ++curGID;
                        }
                        if (isNewGroup) {
                            // new group!
                            mapGIDtoTail->append(curTail);
                        }
                        mapHeadtoGID->append(make_pair(iter->head(), curGID));
                    }
                    delete iter;
                    return make_pair(mapHeadtoGID, mapGIDtoTail);
                }
            };

            /**
             * Group-By the tail (string specialization)
             * Returns 2 BAT's:
             * 1) Mapping (V)OID -> GroupID
             * 2) GroupID -> Value
             */
            template<typename Head>
            struct groupby<Head, v2_str_t> {

                pair<Bat<Head, v2_oid_t>*, Bat<v2_void_t, v2_str_t>*> group(Bat<Head, v2_str_t>* bat) {
                    auto mapHeadtoGID = new TempBat<Head, v2_oid_t>();
                    auto mapGIDtoTail = new TempBat<v2_void_t, v2_str_t>();
                    auto iter = bat->begin();
                    for (; iter->hasNext(); ++*iter) {
                        str_t curTail = iter->tail();
                        // search this tail in our mapping
                        // idx is the void value of mapGIDtoTail, which starts at zero
                        size_t curGID = 0;
                        bool isNewGroup = true;
                        for (auto itertail = mapGIDtoTail->tail.container->begin(); itertail != mapGIDtoTail->tail.container->end(); ++itertail) {
                            if (strcmp(curTail, *itertail) == 0) {
                                isNewGroup = false;
                                break;
                            }
                            ++curGID;
                        }
                        if (isNewGroup) {
                            // new group!
                            mapGIDtoTail->append(curTail);
                        }
                        mapHeadtoGID->append(make_pair(iter->head(), curGID));
                    }
                    delete iter;
                    return make_pair(mapHeadtoGID, mapGIDtoTail);
                }
            };

            template<typename Head, typename Tail>
            pair<Bat<Head, v2_oid_t>*, Bat<v2_void_t, Tail>*> group(Bat<Head, Tail>* bat) {
                return groupby<Head, Tail>().group(bat);
            }

            /**
             * Group by 2 BAT's and sum up one column according to the double-grouping. The OID's in all BAT's must match!
             * @param bat1 The bat over which to sum up
             * @param bat2 The first grouping BAT
             * @param bat3 The second grouping BAT
             * @return Five BATs: 1) sum over double group-by.
             *  2) Mapping sum (V)OID -> group1 OID.
             *  3) Mapping group1 (V)OID -> group1 value.
             *  4) Mapping sum (V)OID -> group2 OID.
             *  5) Mapping group2 (V)OID -> group2 value
             */
            template<typename V2Result, typename Head1, typename Tail1, typename Head2, typename Tail2, typename Head3, typename Tail3>
            tuple<Bat<v2_void_t, V2Result>*, Bat<v2_void_t, v2_oid_t>*, Bat<v2_void_t, Tail2>*, Bat<v2_void_t, v2_oid_t>*, Bat<v2_void_t, Tail3>*> groupedSum(Bat<Head1, Tail1>* bat1, Bat<Head2, Tail2>* bat2, Bat<Head3, Tail3>* bat3) {
#ifdef DEBUG
                StopWatch sw;
                sw.start();
#endif
                auto group1 = group(bat2);
#ifdef DEBUG
                auto time1 = sw.stop();
                sw.start();
#endif
                auto group2 = group(bat3);
#ifdef DEBUG
                auto time2 = sw.stop();
                sw.start();
#endif
                // create an array which can hold enough sums
                auto size1 = group1.second->size();
                auto size2 = group2.second->size();
                auto numgroups = size1 * size2;

                auto sumBat = new TempBat<v2_void_t, V2Result>();
                sumBat->tail.container->resize(numgroups, 0);
                auto outBat2 = new TempBat<v2_void_t, v2_oid_t>(numgroups);
                auto outBat4 = new TempBat<v2_void_t, v2_oid_t>(numgroups);

                auto g1SecondIter = group1.second->begin();
                auto g2SecondIter = group2.second->begin();
                for (; g1SecondIter->hasNext(); ++*g1SecondIter) {
                    g2SecondIter->position(0);
                    for (; g2SecondIter->hasNext(); ++*g2SecondIter) {
                        outBat2->append(g1SecondIter->head());
                        outBat4->append(g2SecondIter->head());
                    }
                }
                delete g1SecondIter;
                delete g2SecondIter;

                auto sums = sumBat->tail.container->data();
                auto iter1 = bat1->begin();
                auto g1FirstIter = group1.first->begin();
                auto g2FirstIter = group2.first->begin();
                for (; iter1->hasNext(); ++*iter1, ++*g1FirstIter, ++*g2FirstIter) {
                    size_t pos = g1FirstIter->tail() + g2FirstIter->tail() * size1;
                    sums[pos] += static_cast<typename V2Result::type_t> (iter1->tail());
                }
                delete iter1;
                delete g1FirstIter;
                delete g2FirstIter;
                delete group1.first;
                delete group2.first;
#ifdef DEBUG
                auto time3 = sw.stop();
                cout << "group1 took " << time1 << " ns. group2 took " << time2 << " ns. grouped sum took " << time3 << " ns." << endl;
#endif
                return make_tuple(sumBat, outBat2, group1.second, outBat4, group2.second);
            }
        }
    }
}

#endif
