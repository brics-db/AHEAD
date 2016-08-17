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

            template<typename Tail>
            void prn_vecOfPair(Bat<v2_oid_t, Tail> *arg) {
                auto iter = arg->begin();
                unsigned step = 0;
                while (iter->hasNext()) {
                    pair<v2_oid_t, Tail> p = iter->next();
                    cout << p.first << " | " << p.second << endl;
                    if (iter->size() > 10 && step == 3) {
                        step = iter->size() - 3;
                        cout << "... jumping to " << step << endl;
                        iter->get(step);
                    }
                    step++;
                }
                delete iter;
            }

            template<typename Head, typename Tail>
            Bat<Head, Tail>* copy(Bat<Head, Tail>* arg, unsigned start = 0, size_t size = 0) {
                auto result = new TempBat<Head, Tail>(arg->size());
                auto iter = arg->begin();
                if (iter->hasNext()) {
                    result->append(iter->get(start));
                    if (size) {
                        for (size_t step = 1; step < size && iter->hasNext(); ++step) {
                            result->append(iter->next());
                        }
                    } else {
                        while (iter->hasNext()) {
                            result->append(iter->next());
                        }
                    }
                }
                delete iter;
                return result; // possibly empty
            }

            template<typename Head, typename Tail, typename TH>
            Bat<Head, Tail>* selection(Bat<Head, Tail>* arg, selection_type_t op, TH&& threshold, TH&& threshold2 = TH(0)) {
                switch (op) {
                    case EQ:
                        return selection_eq(arg, forward(threshold));
                    case LT:
                        return selection_lt(arg, forward(threshold));
                    case LE:
                        return selection_le(arg, forward(threshold));
                    case GT:
                        return selection_gt(arg, forward(threshold));
                    case GE:
                        return selection_ge(arg, forward(threshold));
                    case BT:
                        return selection_bt(arg, forward(threshold), forward(threshold2));
                }
            }

            template<typename Head, typename Tail, typename TH>
            Bat<Head, Tail>* selection_le(Bat<Head, Tail>* arg, TH&& th) {
                typename Tail::type_t threshold = static_cast<typename Tail::type_t> (th);
                auto result = new TempBat<Head, Tail>;
                auto *iter = arg->begin();
                while (iter->hasNext()) {
                    auto p = iter->next();
                    if (p.second <= threshold) {
                        result->append(make_pair(std::move(p.first), std::move(p.second)));
                    }
                }
                delete iter;
                return result;
            }

            template<typename Head, typename Tail, typename TH>
            Bat<Head, Tail>* selection_lt(Bat<Head, Tail>* arg, TH&& th) {
                typename Tail::type_t threshold = static_cast<typename Tail::type_t> (th);
                auto result = new TempBat<Head, Tail>;
                auto *iter = arg->begin();
                while (iter->hasNext()) {
                    auto p = iter->next();
                    if (p.second < threshold) {
                        result->append(make_pair(std::move(p.first), std::move(p.second)));
                    }
                }
                delete iter;
                return result;
            }

            template<typename Head, typename Tail, typename TH>
            Bat<Head, Tail>* selection_bt(Bat<Head, Tail>* arg, TH&& thStart, TH&& thEnd) {
                typename Tail::type_t start = static_cast<typename Tail::type_t> (thStart);
                typename Tail::type_t end = static_cast<typename Tail::type_t> (thEnd);
                auto result = new TempBat<Head, Tail>;
                auto *iter = arg->begin();
                while (iter->hasNext()) {
                    auto p = iter->next();
                    if (p.second <= end && p.second >= start) {
                        result->append(make_pair(std::move(p.first), std::move(p.second)));
                    }
                }
                delete iter;
                return result;
            }

            template<typename Head, typename Tail, typename Val>
            Bat<Head, Tail>* selection_eq(Bat<Head, Tail>* arg, Val&& val) {
                typename Tail::type_t value = static_cast<typename Tail::type_t> (val);
                auto result = new TempBat<Head, Tail>;
                auto iter = arg->begin();
                while (iter->hasNext()) {
                    auto p = iter->next();
                    if (p.second == value) {
                        result->append(make_pair(std::move(p.first), std::move(p.second)));
                    }
                }
                delete iter;
                return result;
            }

            template<typename Head, typename Tail, typename TH>
            Bat<Head, Tail>* selection_gt(Bat<Head, Tail>* arg, TH&& th) {
                typename Tail::type_t threshold = static_cast<typename Tail::type_t> (th);
                auto result = new TempBat<Head, Tail>();
                auto *iter = arg->begin();
                while (iter->hasNext()) {
                    auto p = iter->next();
                    if (p.second > threshold) {
                        result->append(make_pair(std::move(p.first), std::move(p.second)));
                    }
                }
                delete iter;
                return result;
            }

            template<typename Head, typename Tail, typename TH>
            Bat<Head, Tail>* selection_ge(Bat<Head, Tail>* arg, TH&& th) {
                typename Tail::type_t threshold = static_cast<typename Tail::type_t> (th);
                auto result = new TempBat<Head, Tail>();
                auto *iter = arg->begin();
                while (iter->hasNext()) {
                    auto p = iter->next();
                    if (p.second >= threshold) {
                        result->append(make_pair(std::move(p.first), std::move(p.second)));
                    }
                }
                delete iter;
                return result;
            }

            template <typename Head, typename Tail>
            Bat<Tail, Head>* reverse(Bat<Head, Tail> *arg) {
                auto result = new TempBat<Tail, Head>(arg->size());
                auto iter = arg->begin();
                while (iter->hasNext()) {
                    auto p = iter->next();
                    result->append(make_pair(std::move(p.second), std::move(p.first)));
                }
                delete iter;
                return result;
            }

            template<class Head, class Tail>
            Bat<Head, Head>* mirrorHead(Bat<Head, Tail> *arg) {
                auto result = new TempBat<Head, Head>(arg->size());
                auto iter = arg->begin();
                while (iter->hasNext()) {
                    auto p = iter->next();
                    result->append(make_pair(std::move(p.first), std::move(p.first)));
                }
                delete iter;
                return result;
            }

            template<class Head, class Tail>
            Bat<Tail, Tail>* mirrorTail(Bat<Head, Tail> *arg) {
                auto result = new TempBat<Tail, Tail>(arg->size());
                auto iter = arg->begin();
                while (iter->hasNext()) {
                    auto p = iter->next();
                    result->append(make_pair(std::move(p.second), std::move(p.second)));
                }
                delete iter;
                return result;
            }

            template<class T1, class T2, class T3, class T4>
            Bat<T1, T4>* col_nestedloop_join(Bat<T1, T2> *arg1, Bat<T3, T4> *arg2) {
                auto result = new TempBat<T1, T4>();
                auto iter1 = arg1->begin();
                while (iter1->hasNext()) {
                    auto p1 = iter1->next();
                    auto iter2 = arg2->begin();
                    while (iter2->hasNext()) {
                        pair<T3, T4> p2 = iter2->next();
                        if (p1.second == p2.first) {
                            result->append(make_pair(std::move(p1.first), std::move(p2.second)));
                        }
                    }
                    delete iter2;
                }
                delete iter1;
                return result;
            }

            template<class T1, class T2, class T3, class T4>
            Bat<T1, T4>* col_selectjoin(Bat<T1, T2> *arg1, Bat<T3, T4> *arg2) {
                auto result = new TempBat<T1, T4>();
                auto iter1 = arg1->begin();
                auto iter2 = arg2->begin();
                bool working = true;
                while (iter1->hasNext() && iter2->hasNext()) {
                    auto p1 = iter1->next();
                    auto p2 = iter2->next();
                    while (p1.second != p2.first && working) {
                        working = false;
                        while (p1.second < p2.first && iter1->hasNext()) {
                            p1 = iter1->next();
                            working = true;
                        }
                        while (p1.second > p2.first && iter2->hasNext()) {
                            p2 = iter2->next();
                            working = true;
                        }
                    }
                    if (p1.second == p2.first) {
                        result->append(make_pair(std::move(p1.first), std::move(p2.second)));
                    }
                }
                delete iter1;
                delete iter2;
                return result;
            }

            template<class T1, class T2, class T3>
            Bat<T1, T3>* col_hashjoin_old(Bat<T1, T2> *arg1, Bat<T2, T3> *arg2, join_side_t side = join_side_t::left) {
                auto result = new TempBat<T1, T3>();
                auto iter1 = arg1->begin();
                auto iter2 = arg2->begin();
                if (side == join_side_t::left) {
                    unordered_map<typename T2::type_t, vector<typename T1::type_t>* > hashMapLeft;
                    while (iter1->hasNext()) {
                        auto p1 = iter1->next();
                        vector<typename T1::type_t> *vec;
                        if (hashMapLeft.find(p1.second) == hashMapLeft.end()) {
                            hashMapLeft[p1.second] = (vec = new vector<typename T1::type_t>);
                        } else {
                            vec = hashMapLeft[p1.second];
                        }
                        vec->emplace_back(std::move(p1.first));
                    }
                    while (iter2->hasNext()) {
                        auto p2 = iter2->next();
                        if (hashMapLeft.find(p2.first) != hashMapLeft.end()) {
                            auto vec = hashMapLeft[p2.first];
                            for (size_t i = 0; i < vec->size(); i++) {
                                result->append(make_pair(std::move((*vec)[i]), std::move(p2.second)));
                            }
                        }
                    }
                    for (auto elem : hashMapLeft) {
                        delete elem.second;
                    }
                } else {
                    unordered_map<typename T2::type_t, vector<typename T3::type_t>* > hashMapRight;
                    while (iter2->hasNext()) {
                        auto p1 = iter2->next();
                        vector<typename T3::type_t> *vec;
                        if (hashMapRight.find(p1.first) == hashMapRight.end()) {
                            hashMapRight[p1.first] = (vec = new vector<typename T3::type_t>);
                        } else {
                            vec = hashMapRight[p1.first];
                        }
                        vec->emplace_back(std::move(p1.second));
                    }
                    while (iter1->hasNext()) {
                        auto p2 = iter1->next();
                        auto iterMap = hashMapRight.find(p2.second);
                        if (iterMap != hashMapRight.end()) {
                            auto vec = iterMap->second;
                            for (size_t i = 0; i < vec->size(); i++) {
                                result->append(make_pair(std::move((*vec)[i]), std::move(p2.first)));
                            }
                        }
                    }
                    for (auto elem : hashMapRight) {
                        delete elem.second;
                    }
                }
                delete iter1;
                delete iter2;
                return result;
            }

            template<class T1, class T2, class T3>
            Bat<T1, T3>* col_hashjoin(Bat<T1, T2> *arg1, Bat<T2, T3> *arg2, join_side_t side = join_side_t::left) {
                auto result = new TempBat<T1, T3>();
                auto iter1 = arg1->begin();
                auto iter2 = arg2->begin();
                if (iter1->hasNext() && iter2->hasNext()) {
                    if (side == join_side_t::left) {
                        unordered_map<typename T2::type_t, vector<typename T1::type_t> > hashMap;
                        while (iter1->hasNext()) {
                            auto pairLeft = iter1->next();
                            hashMap[pairLeft.second].emplace_back(move(pairLeft.first));
                        }
                        auto mapEnd = hashMap.end();
                        while (iter2->hasNext()) {
                            auto pairRight = iter2->next();
                            auto iterMap = hashMap.find(pairRight.first);
                            if (iterMap != mapEnd) {
                                for (auto matched : iterMap->second) {
                                    result->append(make_pair(matched, pairRight.second));
                                }
                            }
                        }
                    } else {
                        unordered_map<typename T2::type_t, vector<typename T3::type_t> > hashMap;
                        while (iter2->hasNext()) {
                            auto pairRight = iter2->next();
                            hashMap[pairRight.first].emplace_back(pairRight.second);
                        }
                        auto mapEnd = hashMap.end();
                        while (iter1->hasNext()) {
                            auto pairLeft = iter1->next();
                            auto iterMap = hashMap.find(pairLeft.second);
                            if (iterMap != mapEnd) {
                                for (auto matched : iterMap->second) {
                                    result->append(make_pair(pairLeft.first, matched));
                                }
                            }
                        }
                    }
                }
                delete iter1;
                delete iter2;
                return result;
            }

            template<class Tail1, class Tail2>
            Bat<v2_oid_t, Tail2>* col_fill(Bat<v2_oid_t, Tail1> *arg, Tail2 value) {
                auto result = new TempBat<v2_oid_t, Tail2>(arg->size());
                auto iter = arg->begin();
                while (iter->hasNext()) {
                    auto p = iter->next();
                    result->append(make_pair(std::move(p.first), std::move(value)));
                }
                delete iter;
                return result;
            }

            template<class Tail>
            Bat<v2_oid_t, Tail>* col_aggregate_sum(Bat<v2_oid_t, Tail> *arg, Tail initValue) {
                auto result = new TempBat<v2_oid_t, Tail>();
                auto values = new map<v2_oid_t, Tail>();
                auto iter = arg->begin();
                while (iter->hasNext()) {
                    auto p = iter->next();
                    if (values->find(p.first) == values->end()) {
                        (*values)[p.first] = initValue + p.second;
                    } else {
                        (*values)[p.first] = (*values)[p.first] + p.second;
                    }
                }
                delete iter;
                for (auto iter = values->begin(); iter != values->end(); iter++) {
                    result->append(make_pair(std::move(iter->first), std::move(iter->second)));
                }
                delete values;
                return result;
            }

            /**
             * Simple sum operator
             * "valn = sum(val0..valn)"
             * @Author: burkhard
             **/
            template<class Tail>
            Bat<v2_oid_t, Tail>* sum_op(Bat<v2_oid_t, Tail> *arg) {
                auto result = new TempBat<v2_oid_t, Tail>(arg->size());
                auto iter = arg->begin();
                oid_t a = 0; //sum of p.first
                Tail b = (Tail) 0; //sum of p.second
                while (iter->hasNext()) {
                    auto p = iter->next();
                    result->append(make_pair(std::move(a += p.first), std::move(b += p.second)));
                }
                delete iter;
                return result;
            }

            /**
             * Simple sum of all tail values in a Bat
             * @param arg a Bat
             * @return a single sum value
             */
            template<typename Tail>
            Tail aggregate_sum(Bat<v2_oid_t, Tail>* arg) {
                Tail sum = 0;
                auto iter = arg->begin();
                while (iter->hasNext()) {
                    sum += iter->next().second;
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
            template<typename Result, typename Tail1, typename Tail2>
            Result aggregate_mul_sum(Bat<v2_oid_t, Tail1>* arg1, Bat<v2_oid_t, Tail2>* arg2, Result init = Result(0)) {
                auto iter1 = arg1->begin();
                auto iter2 = arg2->begin();
                Result total = init;
                while (iter1->hasNext() && iter2->hasNext()) {
                    total += (static_cast<Result> (iter1->next().second) * static_cast<Result> (iter2->next().second));
                }
                delete iter2;
                delete iter1;
                return total;
            }

            /**
             * Exponential smoothing operator
             * @Author: burkhard
             **/
            template<class Tail>
            Bat<v2_oid_t, Tail>* exp_sm_op(Bat<v2_oid_t, Tail> *arg, double alpha) {
                auto result = new TempBat<v2_oid_t, Tail>(arg->size());
                auto iter = arg->begin();
                bool first = true; //the first element is added as it i.e. without being influenced by alpha
                pair<v2_oid_t, Tail> s; //last pair added to result
                while (iter->hasNext()) {
                    auto p = iter->next(); //current pair
                    if (!first) {
                        s = make_pair(std::move(p.first), std::move((Tail) (alpha * p.second + (1 - alpha) * s.second)));
                    } else {
                        s = p;
                        first = false;
                    }
                    result->append(s);
                }
                delete iter;
                return result;
            }

            /**
             * find_wp_op: returns a Bat with the found WPs
             * at least 1 element needed for a WP and 3 elements between each consecutive WPs
             * the first element after a WP is used to determine the direction (angle)
             * a point is on the same segment iif the angular difference is less or equal to 'tolerance'
             * @Author: burkhard
             **/
            template<class Tail>
            Bat<v2_oid_t, Tail>* find_wp_op(Bat<v2_oid_t, Tail> *arg, double tolerance = 0.5) {
                auto result = new TempBat<v2_oid_t, Tail>();
                auto iter = arg->begin();
                if (!iter->hasNext()) return result; //without elements, no WPs
                auto lastwp = iter->next();
                result->append(lastwp); //the first point is always a WP
                std::cout << "WP: " << lastwp.first << "|" << lastwp.second << std::endl;
                if (!iter->hasNext()) return result; //without a second element we cannot find more WPs
                auto tmp = iter->next();
                double dir = -100; //direction determined by the last WP and the following point. Less than -10 means dir has to be recalculated
                while (iter->hasNext()) {//main loop
                    if (dir < -10) {//WP was added, calculate new dir
                        tmp = iter->next();
                        dir = atan2(tmp.second - lastwp.second, tmp.first - lastwp.first);
                        std::cout << "dir: " << dir << std::endl;
                    }
                    if (!iter->hasNext()) return result; //no more elements
                    tmp = iter->next(); //next element
                    if (fabs(atan2(tmp.second - lastwp.second, tmp.first - lastwp.first) - dir) > tolerance) {//new WP found!
                        lastwp = tmp;
                        result->append(lastwp);
                        std::cout << "WP: " << lastwp.first << "|" << lastwp.second << std::endl;
                        dir = -50; //forces calculation of new direction
                    }
                }
                delete iter;
                result->append(tmp); //last element is always a WP
                return result;
            }

        }
    }
}

#endif
