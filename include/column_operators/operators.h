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
#include <cstdlib>
#include <unordered_map>
#include <cmath>
#include <utility>

#include <ColumnStore.h>
#include <column_storage/Bat.h>
#include <column_storage/TempBat.h>

#define SEL_EQ 1
#define SEL_LT 2
#define SEL_LE 3
#define SEL_GT 4
#define SEL_GE 5
#define SEL_BT 6

#define JOIN_LEFT 0
#define JOIN_RIGHT 1

using namespace std;

namespace v2 {
    namespace bat {
        namespace ops {

            template<class Tail>
            void prn_vecOfPair(Bat<oid_t, Tail> *arg) {
                auto iter = arg->begin();
                unsigned step = 0;
                while (iter->hasNext()) {
                    pair<oid_t, Tail> p = iter->next();
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

            template<class Tail>
            Bat<oid_t, Tail>* copy(Bat<oid_t, Tail>* arg, unsigned start = 0, size_t size = 0) {
                auto result = new TempBat<oid_t, Tail>(arg->size());
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

            template<class Tail>
            Bat<oid_t, Tail>* selection(Bat<oid_t, Tail>* arg, unsigned OP, Tail treshold, Tail treshold2 = Tail(0)) {
                switch (OP) {
                    case SEL_EQ:
                        return selection_eq(arg, treshold);
                    case SEL_LT:
                        return selection_lt(arg, treshold);
                    case SEL_LE:
                        return selection_le(arg, treshold);
                    case SEL_GT:
                        return selection_gt(arg, treshold);
                    case SEL_GE:
                        return selection_ge(arg, treshold);
                    case SEL_BT:
                        return selection_bt(arg, treshold, treshold2);
                }
            }

            template<class Tail>
            Bat<oid_t, Tail>* selection_le(Bat<oid_t, Tail>* arg, Tail treshold) {
                auto result = new TempBat<oid_t, Tail>();
                auto *iter = arg->begin();
                while (iter->hasNext()) {
                    pair<oid_t, Tail> p = iter->next();
                    if (p.second <= treshold) {
                        result->append(make_pair(std::move(p.first), std::move(p.second)));
                    }
                }
                delete iter;
                return result;
            }

            template<class Tail>
            Bat<oid_t, Tail>* selection_lt(Bat<oid_t, Tail>* arg, Tail treshold) {
                auto result = new TempBat<oid_t, Tail>();
                auto *iter = arg->begin();
                while (iter->hasNext()) {
                    pair<oid_t, Tail> p = iter->next();
                    if (p.second < treshold) {
                        result->append(make_pair(std::move(p.first), std::move(p.second)));
                    }
                }
                delete iter;
                return result;
            }

            template<class Tail>
            Bat<oid_t, Tail>* selection_bt(Bat<oid_t, Tail>* arg, Tail start, Tail end) {
                auto result = new TempBat<oid_t, Tail>();
                auto *iter = arg->begin();
                while (iter->hasNext()) {
                    pair<oid_t, Tail> p = iter->next();
                    if (p.second <= end && p.second >= start) {
                        result->append(make_pair(std::move(p.first), std::move(p.second)));
                    }
                }
                delete iter;
                return result;
            }

            template<class Tail>
            Bat<oid_t, Tail>* selection_eq(Bat<oid_t, Tail>* arg, Tail value) {
                auto result = new TempBat<oid_t, Tail>();
                auto iter = arg->begin();
                while (iter->hasNext()) {
                    pair<oid_t, Tail> p = iter->next();
                    if (p.second == value) {
                        result->append(make_pair(std::move(p.first), std::move(p.second)));
                    }
                }
                delete iter;
                return result;
            }

            template<class Tail>
            Bat<oid_t, Tail>* selection_gt(Bat<oid_t, Tail>* arg, Tail treshold) {
                auto result = new TempBat<oid_t, Tail>();
                auto *iter = arg->begin();
                while (iter->hasNext()) {
                    pair<oid_t, Tail> p = iter->next();
                    if (p.second > treshold) {
                        result->append(make_pair(std::move(p.first), std::move(p.second)));
                    }
                }
                delete iter;
                return result;
            }

            template<class Tail>
            Bat<oid_t, Tail>* selection_ge(Bat<oid_t, Tail>* arg, Tail treshold) {
                auto result = new TempBat<oid_t, Tail>();
                auto *iter = arg->begin();
                while (iter->hasNext()) {
                    pair<oid_t, Tail> p = iter->next();
                    if (p.second >= treshold) {
                        result->append(make_pair(std::move(p.first), std::move(p.second)));
                    }
                }
                delete iter;
                return result;
            }

            template <class Tail>
            Bat<Tail, oid_t>* reverse(Bat<oid_t, Tail> *arg) {
                auto result = new TempBat<Tail, oid_t>(arg->size());
                auto iter = arg->begin();
                while (iter->hasNext()) {
                    pair<oid_t, Tail> p = iter->next();
                    result->append(make_pair(std::move(p.second), std::move(p.first)));
                }
                delete iter;
                return result;
            }

            template<class Tail>
            Bat<oid_t, oid_t>* mirror(Bat<oid_t, Tail> *arg) {
                auto result = new TempBat<oid_t, oid_t>(arg->size());
                auto iter = arg->begin();
                while (iter->hasNext()) {
                    pair<oid_t, Tail> p = iter->next();
                    result->append(make_pair(std::move(p.first), std::move(p.first)));
                }
                delete iter;
                return result;
            }

            template<class T1, class T2, class T3, class T4>
            Bat<T1, T4>* col_nestedloop_join(Bat<T1, T2> *arg1, Bat<T3, T4> *arg2) {
                auto result = new TempBat<T1, T4>();
                auto iter1 = arg1->begin();
                while (iter1->hasNext()) {
                    pair<T1, T2> p1 = iter1->next();
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
                pair<T1, T2> p1;
                pair<T3, T4> p2;
                bool working = true;
                while (iter1->hasNext() && iter2->hasNext()) {
                    p1 = iter1->next();
                    p2 = iter2->next();
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
            Bat<T1, T3>* col_hashjoin(Bat<T1, T2> *arg1, Bat<T2, T3> *arg2, int SIDE = 0) {
                auto result = new TempBat<T1, T3>();
                auto iter1 = arg1->begin();
                auto iter2 = arg2->begin();
                unordered_map<T2, vector<T1>* > hashMapLeft;
                unordered_map<T2, vector<T3>* > hashMapRight;
                if (SIDE == JOIN_LEFT) {
                    while (iter1->hasNext()) {
                        auto p1 = iter1->next();
                        vector<T1> *vec;
                        if (hashMapLeft.find(p1.second) == hashMapLeft.end()) {
                            vec = new vector<T1>();
                        } else {
                            vec = (vector<T1>*)hashMapLeft[(T2) p1.second];
                        }
                        vec->emplace_back(std::move(p1.first));
                        hashMapLeft[p1.second] = vec;
                    }
                } else {
                    while (iter2->hasNext()) {
                        pair<T2, T3> p1 = iter2->next();
                        vector<T3> *vec;
                        if (hashMapRight.find(p1.first) == hashMapRight.end()) {
                            vec = new vector<T3>();
                        } else {
                            vec = (vector<T3>*)hashMapRight[(T2) p1.first];
                        }
                        vec->emplace_back(std::move(p1.second));
                        hashMapRight[p1.first] = vec;
                    }
                }
                // probing against hash map
                if (SIDE == JOIN_LEFT) {
                    while (iter2->hasNext()) {
                        auto p2 = iter2->next();
                        if (hashMapLeft.find((T2) p2.first) != hashMapLeft.end()) {
                            vector<T1>* vec = (vector<T1>*)hashMapLeft[(T2) p2.first];
                            for (size_t i = 0; i < vec->size(); i++) {
                                result->append(make_pair(std::move((*vec)[i]), std::move(p2.second)));
                            }
                        }
                    }
                } else {
                    while (iter1->hasNext()) {
                        pair<T1, T2> p2 = iter1->next();
                        if (hashMapRight.find((T2) p2.second) != hashMapRight.end()) {
                            vector<T3>* vec = (vector<T3>*)hashMapRight[(T3) p2.second];
                            for (size_t i = 0; i < vec->size(); i++) {
                                result->append(make_pair(std::move((*vec)[i]), std::move(p2.first)));
                            }
                        }
                    }

                }
                for (auto elem : hashMapLeft) {
                    delete elem.second;
                }
                for (auto elem : hashMapRight) {
                    delete elem.second;
                }
                delete iter1;
                delete iter2;
                return result;
            }

            template<class Tail1, class Tail2>
            Bat<oid_t, Tail2>* col_fill(Bat<oid_t, Tail1> *arg, Tail2 value) {
                auto result = new TempBat<oid_t, Tail2>(arg->size());
                auto iter = arg->begin();
                while (iter->hasNext()) {
                    auto p = iter->next();
                    result->append(make_pair(std::move(p.first), std::move(value)));
                }
                delete iter;
                return result;
            }

            template<class Tail>
            Bat<oid_t, Tail>* col_aggregate_sum(Bat<oid_t, Tail> *arg, Tail initValue) {
                auto result = new TempBat<oid_t, Tail>();
                auto values = new map<oid_t, Tail>();
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
            Bat<oid_t, Tail>* sum_op(Bat<oid_t, Tail> *arg) {
                auto result = new TempBat<oid_t, Tail>(arg->size());
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
            Tail aggregate_sum(Bat<oid_t, Tail>* arg) {
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
            Result aggregate_mul_sum(Bat<oid_t, Tail1>* arg1, Bat<oid_t, Tail2>* arg2, Result init) {
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
            Bat<oid_t, Tail>* exp_sm_op(Bat<oid_t, Tail> *arg, double alpha) {
                auto result = new TempBat<oid_t, Tail>(arg->size());
                auto iter = arg->begin();
                bool first = true; //the first element is added as it i.e. without being influenced by alpha
                pair<oid_t, Tail> s; //last pair added to result
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
            Bat<oid_t, Tail>* find_wp_op(Bat<oid_t, Tail> *arg, double tolerance = 0.5) {
                auto result = new TempBat<oid_t, Tail>();
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
