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
                unsigned step = 0;
                auto *iter = arg->begin();
                for (; iter->hasNext(); ++*iter) {
                    cout << iter->head() << " | " << iter->tail() << endl;
                    if (iter->size() > 10 && step == 3) {
                        step = iter->size() - 3;
                        cout << "... jumping to " << step << endl;
                        iter->position(step);
                    }
                    step++;
                }
                delete iter;
            }

            template<typename Head, typename Tail>
            Bat<Head, Tail>* copy(Bat<Head, Tail>* arg, unsigned start = 0, size_t size = 0) {
                auto result = new TempBat<Head, Tail>(arg->size());
                auto *iter = arg->begin();
                if (iter->hasNext()) {
                    iter->position(start);
                    result->append(make_pair(iter->head(), iter->tail()));
                    if (size) {
                        for (size_t step = 1; step < size && iter->hasNext(); ++step) {
                            result->append(make_pair(iter->head(), iter->tail()));
                            ++*iter;
                        }
                    } else {
                        for (; iter->hasNext(); ++*iter) {
                            result->append(make_pair(iter->head(), iter->tail()));
                        }
                    }
                }
                delete iter;
                return result; // possibly empty
            }

            template<typename Head, typename Tail, typename TH>
            Bat<v2_oid_t, Tail>* selection(Bat<Head, Tail>* arg, selection_type_t op, TH&& threshold, TH&& threshold2 = TH(0)) {
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
            Bat<v2_oid_t, Tail>* selection_le(Bat<Head, Tail>* arg, TH&& th) {
                typename Tail::type_t threshold = static_cast<typename Tail::type_t> (th);
                auto result = new TempBat<v2_oid_t, Tail>;
                auto *iter = arg->begin();
                for (; iter->hasNext(); ++*iter) {
                    auto t = iter->tail();
                    if (t <= threshold) {
                        result->append(make_pair(iter->head(), t));
                    }
                }
                delete iter;
                return result;
            }

            template<typename Head, typename Tail, typename TH>
            Bat<v2_oid_t, Tail>* selection_lt(Bat<Head, Tail>* arg, TH&& th) {
                typename Tail::type_t threshold = static_cast<typename Tail::type_t> (th);
                auto result = new TempBat<v2_oid_t, Tail>;
                auto *iter = arg->begin();
                for (; iter->hasNext(); ++*iter) {
                    auto t = iter->tail();
                    if (t < threshold) {
                        result->append(make_pair(iter->head(), t));
                    }
                }
                delete iter;
                return result;
            }

            template<typename Head, typename Tail, typename TH>
            Bat<v2_oid_t, Tail>* selection_bt(Bat<Head, Tail>* arg, TH&& thStart, TH&& thEnd) {
                typename Tail::type_t start = static_cast<typename Tail::type_t> (thStart);
                typename Tail::type_t end = static_cast<typename Tail::type_t> (thEnd);
                auto result = new TempBat<v2_oid_t, Tail>;
                auto *iter = arg->begin();
                for (; iter->hasNext(); ++*iter) {
                    auto t = iter->tail();
                    if (t <= end && t >= start) {
                        result->append(make_pair(iter->head(), t));
                    }
                }
                delete iter;
                return result;
            }

            template<typename Head, typename Tail, typename Val>
            Bat<v2_oid_t, Tail>* selection_eq(Bat<Head, Tail>* arg, Val&& val) {
                typename Tail::type_t value = static_cast<typename Tail::type_t> (val);
                auto result = new TempBat<v2_oid_t, Tail>;
                auto *iter = arg->begin();
                for (; iter->hasNext(); ++*iter) {
                    auto t = iter->tail();
                    if (t == value) {
                        result->append(make_pair(iter->head(), t));
                    }
                }
                delete iter;
                return result;
            }

            template<typename Head, typename Tail, typename TH>
            Bat<v2_oid_t, Tail>* selection_gt(Bat<Head, Tail>* arg, TH&& th) {
                typename Tail::type_t threshold = static_cast<typename Tail::type_t> (th);
                auto result = new TempBat<v2_oid_t, Tail>();
                auto *iter = arg->begin();
                for (; iter->hasNext(); ++*iter) {
                    auto t = iter->tail();
                    if (t > threshold) {
                        result->append(make_pair(iter->head(), t));
                    }
                }
                delete iter;
                return result;
            }

            template<typename Head, typename Tail, typename TH>
            Bat<v2_oid_t, Tail>* selection_ge(Bat<Head, Tail>* arg, TH&& th) {
                typename Tail::type_t threshold = static_cast<typename Tail::type_t> (th);
                auto result = new TempBat<v2_oid_t, Tail>();
                auto *iter = arg->begin();
                for (; iter->hasNext(); ++*iter) {
                    auto t = iter->tail();
                    if (t >= threshold) {
                        result->append(make_pair(iter->head(), t));
                    }
                }
                delete iter;
                return result;
            }

            template <typename Head, typename Tail>
            Bat<Tail, Head>* reverse(Bat<Head, Tail> *arg) {
                auto result = new TempBat<Tail, Head>();
                auto iter = arg->begin();
                for (; iter->hasNext(); ++*iter) {
                    result->append(make_pair(iter->tail(), iter->head()));
                }
                delete iter;
                return result;
            }

            template<class Head, class Tail>
            Bat<Head, Head>* mirrorHead(Bat<Head, Tail> *arg) {
                auto result = new TempBat<Head, Head>(arg->size());
                auto iter = arg->begin();
                for (; iter->hasNext(); ++*iter) {
                    auto h = iter->head();
                    result->append(make_pair(h, h));
                }
                delete iter;
                return result;
            }

            template<class Head, class Tail>
            Bat<Tail, Tail>* mirrorTail(Bat<Head, Tail> *arg) {
                auto result = new TempBat<Tail, Tail>(arg->size());
                auto iter = arg->begin();
                for (; iter->hasNext(); ++*iter) {
                    auto t = iter->tail();
                    result->append(make_pair(t, t));
                }
                delete iter;
                return result;
            }

            template<class T1, class T2, class T3, class T4>
            Bat<T1, T4>* col_nestedloop_join(Bat<T1, T2> *arg1, Bat<T3, T4> *arg2) {
                auto result = new TempBat<T1, T4>();
                auto iter1 = arg1->begin();
                while (iter1->hasNext()) {
                    auto h = iter1->head();
                    auto iter2 = arg2->begin();
                    while (iter2->hasNext()) {
                        pair<T3, T4> p2 = iter2->next();
                        if (iter1->tail() == p2.first) {
                            result->append(make_pair(move(h), move(iter2->tail())));
                        }
                    }
                    delete iter2;
                    iter1->next();
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
                for (; iter1->hasNext() && iter2->hasNext(); ++*iter1, ++*iter2) {
                    auto t1 = iter1->tail();
                    auto h2 = iter2->head();
                    while (t1 != h2 && working) {
                        working = false;
                        while (t1 < h2 && iter1->hasNext()) {
                            ++*iter1;
                            t1 = iter1->tail();
                            working = true;
                        }
                        while (t1 > h2 && iter2->hasNext()) {
                            ++*iter2;
                            h2 = iter2->head();
                            working = true;
                        }
                    }
                    if (t1 == h2) {
                        result->append(make_pair(move(iter1->head()), move(iter2->tail())));
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
                    for (; iter1->hasNext(); ++*iter1) {
                        auto t1 = iter1->tail();
                        vector<typename T1::type_t> *vec;
                        if (hashMapLeft.find(t1) == hashMapLeft.end()) {
                            hashMapLeft[t1] = (vec = new vector<typename T1::type_t>);
                        } else {
                            vec = hashMapLeft[t1];
                        }
                        vec->emplace_back(move(iter1->head()));
                    }
                    for (; iter2->hasNext(); ++*iter2) {
                        if (hashMapLeft.find(iter2->head()) != hashMapLeft.end()) {
                            auto t2 = iter2->tail();
                            auto vec = hashMapLeft[iter2->head()];
                            for (size_t i = 0; i < vec->size(); i++) {
                                result->append(make_pair(move((*vec)[i]), move(t2)));
                            }
                        }
                    }
                    for (auto elem : hashMapLeft) {
                        delete elem.second;
                    }
                } else {
                    unordered_map<typename T2::type_t, vector<typename T3::type_t>* > hashMapRight;
                    for (; iter2->hasNext(); ++*iter2) {
                        auto h2 = iter2->head();
                        vector<typename T3::type_t> *vec;
                        if (hashMapRight.find(h2) == hashMapRight.end()) {
                            hashMapRight[h2] = (vec = new vector<typename T3::type_t>);
                        } else {
                            vec = hashMapRight[h2];
                        }
                        vec->emplace_back(move(iter2->tail()));
                    }
                    for (; iter1->hasNext(); ++*iter1) {
                        auto iterMap = hashMapRight.find(iter1->tail());
                        if (iterMap != hashMapRight.end()) {
                            auto h1 = iter1->head();
                            auto vec = iterMap->second;
                            for (size_t i = 0; i < vec->size(); i++) {
                                result->append(make_pair(move((*vec)[i]), move(h1)));
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

            template<typename T1, typename T2, typename T3, typename T4>
            Bat<T1, T4>* col_hashjoin(Bat<T1, T2> *arg1, Bat<T3, T4> *arg2, join_side_t side = join_side_t::left) {
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
                        unordered_map<typename T2::type_t, vector<typename T3::type_t> > hashMap;
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

            template<class Head, class Tail1, class Tail2>
            Bat<Head, Tail2>* col_fill(Bat<Head, Tail1> *arg, Tail2 value) {
                auto result = new TempBat<Head, Tail2>(arg->size());
                auto iter = arg->begin();
                for (; iter->hasNext(); ++*iter) {
                    result->append(make_pair(move(iter->head()), move(value)));
                }
                delete iter;
                return result;
            }

            template<class Head, class Tail>
            Bat<Head, Tail>* col_aggregate_sum(Bat<Head, Tail> *arg, Tail initValue) {
                auto result = new TempBat<Head, Tail>();
                auto values = new map<Head, Tail>();
                auto iter = arg->begin();
                for (; iter->hasNext(); ++*iter) {
                    auto h = iter->head();
                    if (values->find(h) == values->end()) {
                        (*values)[h] = initValue + iter->tail();
                    } else {
                        (*values)[h] = (*values)[h] + iter->tail();
                    }
                }
                delete iter;
                for (auto v : values) {
                    result->append(v);
                }
                delete values;
                return result;
            }

            /**
             * Simple sum operator
             * "valn = sum(val0..valn)"
             * @Author: burkhard
             **/
            template<class Head, class Tail>
            Bat<v2_oid_t, Tail>* sum_op(Bat<Head, Tail> *arg) {
                auto result = new TempBat<Head, Tail>(arg->size());
                auto iter = arg->begin();
                oid_t a = 0; //sum of p.first
                Tail b = (Tail) 0; //sum of p.second
                for (; iter->hasNext(); ++*iter) {
                    result->append(make_pair(move(a += iter->head()), move(b += iter->tail())));
                }
                delete iter;
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
             * Exponential smoothing operator
             * @Author: burkhard
             **/
            template<class Head, class Tail>
            Bat<Head, Tail>* exp_sm_op(Bat<Head, Tail> *arg, double alpha) {
                auto result = new TempBat<Head, Tail>(arg->size());
                auto iter = arg->begin();
                bool first = true; //the first element is added as it i.e. without being influenced by alpha
                pair<v2_oid_t, Tail> s; //last pair added to result
                for (; iter->hasNext(); ++*iter) {
                    if (!first) {
                        s = make_pair(move(iter->head()), move((Tail) (alpha * iter->tail() + (1 - alpha) * s.second)));
                    } else {
                        s = make_pair(move(iter->head()), move(iter->tail()));
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
            template<class Head, class Tail>
            Bat<Head, Tail>* find_wp_op(Bat<Head, Tail> *arg, double tolerance = 0.5) {
                auto result = new TempBat<Head, Tail>();
                auto iter = arg->begin();
                if (!iter->hasNext()) return result; //without elements, no WPs
                auto lastwp = make_pair(move(iter->head()), move(iter->tail()));
                result->append(lastwp); //the first point is always a WP
                std::cout << "WP: " << lastwp.first << "|" << lastwp.second << std::endl;
                ++*iter;
                if (!iter->hasNext()) return result; //without a second element we cannot find more WPs
                double dir = -100; //direction determined by the last WP and the following point. Less than -10 means dir has to be recalculated
                auto tmp = lastwp;
                for (; iter->hasNext(); ++*iter) {//main loop
                    tmp.first = iter->head();
                    tmp.second = iter->tail();
                    if (dir < -10) {//WP was added, calculate new dir
                        dir = atan2(iter->tail() - lastwp.second, tmp.first - lastwp.first);
                        std::cout << "dir: " << dir << std::endl;
                    }
                    if (!iter->hasNext()) return result; //no more elements
                    ++*iter;
                    tmp.first = iter->head();
                    tmp.second = iter->tail();
                    if (fabs(atan2(tmp.second - lastwp.second, tmp.first - lastwp.first) - dir) > tolerance) {//new WP found!
                        lastwp = tmp;
                        result->append(lastwp);
                        std::cout << "WP: " << lastwp.first << "|" << lastwp.second << std::endl;
                        dir = -50; //forces calculation of new direction
                    }
                }
                result->append(tmp); //last element is always a WP
                delete iter;
                return result;
            }

        }
    }
}

#endif
