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
} join_side_t;

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

            template<template <typename> class Op, typename Head, typename Tail, typename TH>
            Bat<typename Head::v2_select_t, typename Tail::v2_select_t>* select(Bat<Head, Tail>* arg, TH&& th1) {
                Selection1 < Op<typename Tail::type_t>, Head, Tail, TH> impl;
                return impl(arg, move(th1));
            }

            template<typename Head, typename Tail, typename TH>
            Bat<typename Head::v2_select_t, typename Tail::v2_select_t>* select_bt(Bat<Head, Tail>* arg, TH&& th1, TH&& th2) {
                Selection2 < greater_equal<typename Tail::type_t>, less_equal<typename Tail::type_t>, Head, Tail, TH> impl;
                return impl(arg, move(th1), move(th2));
            }

            template<typename T1, typename T2, typename T3, typename T4>
            Bat<T1, T4>* hashjoin(Bat<T1, T2> *arg1, Bat<T3, T4> *arg2, join_side_t side = join_side_t::left) {
                auto result = new TempBat<T1, T4>();
                auto iter1 = arg1->begin();
                auto iter2 = arg2->begin();
                if (iter1->hasNext() && iter2->hasNext()) {
                    if (side == join_side_t::left) {
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

        }
    }
}

#endif
