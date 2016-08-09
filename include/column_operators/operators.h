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

            template<class Head, class Tail>
            static void prn_vecOfPair(Bat<Head, Tail> *arg) {
                BatIterator<Head, Tail> * iter = arg->begin();

                unsigned step = 0;
                while (iter->hasNext()) {
                    pair<Head, Tail> p = iter->next();
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

            template<class Head, class Tail>
            static Bat<Head, Tail>* copy(Bat<Head, Tail>* arg, unsigned start = 0, size_t size = 0) {
                auto result = new TempBat<Head, Tail>(arg->size());
                BatIterator<Head, Tail> *vcpi = arg->begin();
                if (vcpi->hasNext()) {
                    result->append(vcpi->get(start));
                    if (size) {
                        for (size_t step = 1; step < size && vcpi->hasNext(); ++step) {
                            result->append(vcpi->next());
                        }
                    } else {
                        while (vcpi->hasNext()) {
                            result->append(vcpi->next());
                        }
                    }
                }
                return result; // possibly empty
            }

            template<class Head, class Tail>
            static Bat<Head, Tail>* selection(Bat<Head, Tail>* arg, unsigned OP, Tail treshold, Tail treshold2 = Tail(0)) {
                auto result = new TempBat<Head, Tail>();
                BatIterator<Head, Tail> *iter = arg->begin();
                switch (OP) {
                    case SEL_EQ:
                        while (iter->hasNext()) {
                            pair<Head, Tail> p = iter->next();
                            if (p.second == treshold) {
                                pair<Head, Tail> np;
                                memcpy(&np.first, &p.first, sizeof (Head));
                                memcpy(&np.second, &p.second, sizeof (Tail));
                                result->append(np);
                            }
                        }
                        break;
                    case SEL_LT:
                        while (iter->hasNext()) {
                            pair<Head, Tail> p = iter->next();
                            if (p.second < treshold) {
                                pair<Head, Tail> np;
                                memcpy(&np.first, &p.first, sizeof (Head));
                                memcpy(&np.second, &p.second, sizeof (Tail));
                                result->append(np);
                            }
                        }
                        break;
                    case SEL_LE:
                        while (iter->hasNext()) {
                            pair<Head, Tail> p = iter->next();
                            if (p.second <= treshold) {
                                pair<Head, Tail> np;
                                memcpy(&np.first, &p.first, sizeof (Head));
                                memcpy(&np.second, &p.second, sizeof (Tail));
                                result->append(np);
                            }
                        }
                        break;
                    case SEL_GT:
                        while (iter->hasNext()) {
                            pair<Head, Tail> p = iter->next();
                            if (p.second > treshold) {
                                pair<Head, Tail> np;
                                memcpy(&np.first, &p.first, sizeof (Head));
                                memcpy(&np.second, &p.second, sizeof (Tail));
                                result->append(np);
                            }
                        }
                        break;
                    case SEL_GE:
                        while (iter->hasNext()) {
                            pair<Head, Tail> p = iter->next();
                            if (p.second >= treshold) {
                                pair<Head, Tail> np;
                                memcpy(&np.first, &p.first, sizeof (Head));
                                memcpy(&np.second, &p.second, sizeof (Tail));
                                result->append(np);
                            }
                        }
                        break;
                    case SEL_BT:
                        while (iter->hasNext()) {
                            pair<Head, Tail> p = iter->next();
                            if (p.second >= treshold && p.second <= treshold2) {
                                pair<Head, Tail> np;
                                memcpy(&np.first, &p.first, sizeof (Head));
                                memcpy(&np.second, &p.second, sizeof (Tail));
                                result->append(np);
                            }
                        }
                        break;
                }

                delete iter;

                return result;
            }

            template<class Head, class Tail>
            static Bat<Head, Tail>* selection_lq(Bat<Head, Tail>* arg, Tail treshold) {
                auto result = new TempBat<Head, Tail>();
                BatIterator<Head, Tail> *iter = arg->begin();
                while (iter->hasNext()) {
                    pair<Head, Tail> p = iter->next();
                    if (p.second <= treshold) {
                        pair<Head, Tail> np;
                        memcpy(&np.first, &p.first, sizeof (Head));
                        memcpy(&np.second, &p.second, sizeof (Tail));
                        result->append(np);
                    }
                }

                delete iter;

                return result;
            }

            template<class Head, class Tail>
            static Bat<Head, Tail>* selection_lt(Bat<Head, Tail>* arg, Tail treshold) {
                auto result = new TempBat<Head, Tail>();
                BatIterator<Head, Tail> *iter = arg->begin();
                while (iter->hasNext()) {
                    pair<Head, Tail> p = iter->next();
                    if (p.second < treshold) {
                        pair<Head, Tail> np;
                        memcpy(&np.first, &p.first, sizeof (Head));
                        memcpy(&np.second, &p.second, sizeof (Tail));
                        result->append(np);
                    }
                }

                delete iter;

                return result;
            }

            template<class Head, class Tail>
            static Bat<Head, Tail>* selection_bt(Bat<Head, Tail>* arg, Tail start, Tail end) {
                auto result = new TempBat<Head, Tail>();
                BatIterator<Head, Tail> *iter = arg->begin();
                while (iter->hasNext()) {
                    pair<Head, Tail> p = iter->next();
                    if (p.second <= end && p.second >= start) {
                        pair<Head, Tail> np;
                        memcpy(&np.first, &p.first, sizeof (Head));
                        memcpy(&np.second, &p.second, sizeof (Tail));
                        result->append(np);
                    }
                }

                delete iter;

                return result;
            }

            template<class Head, class Tail>
            static Bat<Head, Tail>* selection_eq(Bat<Head, Tail>* arg, Tail value) {
                auto result = new TempBat<Head, Tail>();
                auto iter = arg->begin();
                while (iter->hasNext()) {
                    pair<Head, Tail> p = iter->next();
                    if (p.second == value) {
                        // pair<Head, Tail> np;
                        // memcpy(&np.first, &p.first, sizeof (Head));
                        // memcpy(&np.second, &p.second, sizeof (Tail));
                        result->append(make_pair(p.first, p.second));
                    }
                }
                delete iter;
                return result;
            }

            template <class Head, class Tail>
            static Bat<Tail, Head>* reverse(Bat<Head, Tail> *arg) {
                auto result = new TempBat<Tail, Head>(arg->size());
                auto iter = arg->begin();
                while (iter->hasNext()) {
                    pair<Head, Tail> p = iter->next();
                    // pair<Tail, Head> np;
                    // memcpy(&np.first, &p.second, sizeof (Tail));
                    // memcpy(&np.second, &p.first, sizeof (Head));
                    result->append(make_pair(p.second, p.first));
                }
                delete iter;
                return result;
            }

            template<class Head, class Tail>
            static Bat<Head, Head>* mirror(Bat<Head, Tail> *arg) {
                auto result = new TempBat<Head, Head>(arg->size());
                auto iter = arg->begin();
                while (iter->hasNext()) {
                    pair<Head, Tail> p = iter->next();
                    // pair<Head, Head> np;
                    // memcpy(&np.first, &p.first, sizeof (Head));
                    // memcpy(&np.second, &p.first, sizeof (Head));
                    result->append(make_pair(p.first, p.first));
                }
                delete iter;
                return result;
            }

            template<class T1, class T2, class T3, class T4>
            static Bat<T1, T4>* col_nestedloop_join(Bat<T1, T2> *arg1, Bat<T3, T4> *arg2) {
                auto result = new TempBat<T1, T4>();
                BatIterator<T1, T2> *iter1 = arg1->begin();
                while (iter1->hasNext()) {
                    pair<T1, T2> p1 = iter1->next();

                    BatIterator<T3, T4> *iter2 = arg2->begin();
                    while (iter2->hasNext()) {
                        pair<T3, T4> p2 = iter2->next();
                        if (p1.second == p2.first) {
                            pair<T1, T4> np;
                            memcpy(&np.first, &p1.first, sizeof (T1));
                            memcpy(&np.second, &p2.second, sizeof (T4));
                            result->append(np);
                        }
                    }
                    delete iter2;
                }
                delete iter1;
                //cout<<arg1->size()<<"x"<<arg2->size()<<"="<<result->size();
                return result;
            }

            template<class T1, class T2, class T3, class T4>
            static Bat<T1, T4>* col_selectjoin(Bat<T1, T2> *arg1, Bat<T3, T4> *arg2) {
                auto result = new TempBat<T1, T4>();
                BatIterator<T1, T2> *iter1 = arg1->begin();
                BatIterator<T3, T4> *iter2 = arg2->begin();
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
                    //cout <<p1.second<<" "<<p2.first<<endl;
                    if (p1.second == p2.first) {
                        // pair<T1, T4> np;
                        // memcpy(&np.first, &p1.first, sizeof (T1));
                        // memcpy(&np.second, &p2.second, sizeof (T4));
                        result->append(make_pair(p1.first, p2.second));
                    }
                }
                delete iter1;
                delete iter2;
                return result;
            }

            template<class T1, class T2, class T3>
            static Bat<T1, T3>* col_hashjoin(Bat<T1, T2> *arg1, Bat<T2, T3> *arg2, int SIDE = 0) {
                //cout<<"hashjoin must be checked - strange errors while compiling indicate copy n paste errors"<<endl;
                auto result = new TempBat<T1, T3>();
                BatIterator<T1, T2> *iter1 = arg1->begin();
                BatIterator<T2, T3> *iter2 = arg2->begin();
                unordered_map<T2, vector<T1>* > hashMapLeft;
                unordered_map<T2, vector<T3>* > hashMapRight;
                // building hash map
                if (SIDE == JOIN_LEFT) {
                    while (iter1->hasNext()) {
                        pair<T1, T2> p1 = iter1->next();
                        if (hashMapLeft.find(p1.second) == hashMapLeft.end()) {
                            vector<T1> *vec = new vector<T1>();
                            vec->emplace_back(p1.first);
                            hashMapLeft[p1.second] = vec;
                        } else {
                            vector<T1>* vec = (vector<T1>*)hashMapLeft[(T2) p1.second];
                            vec->emplace_back(p1.first);
                            hashMapLeft[p1.second] = vec;
                        }
                    }
                } else {
                    while (iter2->hasNext()) {
                        pair<T2, T3> p1 = iter2->next();
                        if (hashMapRight.find(p1.first) == hashMapRight.end()) {
                            vector<T3> *vec = new vector<T3>();
                            vec->emplace_back(p1.second);
                            hashMapRight[p1.first] = vec;
                        } else {
                            vector<T3>* vec = (vector<T3>*)hashMapRight[(T2) p1.first];
                            vec->emplace_back(p1.second);
                            hashMapRight[p1.first] = vec;
                        }
                    }
                }
                // probing against hash map
                if (SIDE == JOIN_LEFT) {
                    while (iter2->hasNext()) {
                        pair<T2, T3> p2 = iter2->next();
                        if (hashMapLeft.find((T2) p2.first) != hashMapLeft.end()) {
                            vector<T1>* vec = (vector<T1>*)hashMapLeft[(T2) p2.first];
                            for (unsigned i = 0; i < (unsigned) vec->size(); i++) {
                                pair<T1, T3> np;
                                T1 value = (T1) (*vec)[i];
                                //cout <<value<<" "<<p2.second<<endl;
                                memcpy(&np.first, &value, sizeof (T1));
                                memcpy(&np.second, &p2.second, sizeof (T3));
                                result->append(np);
                            }
                        }
                    }
                } else {
                    while (iter1->hasNext()) {
                        pair<T1, T2> p2 = iter1->next();
                        if (hashMapRight.find((T2) p2.second) != hashMapRight.end()) {
                            vector<T3>* vec = (vector<T3>*)hashMapRight[(T3) p2.second];
                            for (unsigned i = 0; i < (unsigned) vec->size(); i++) {
                                pair<T1, T3> np;
                                T3 value = (T3) (*vec)[i];
                                //cout <<value<<" "<<p2.second<<endl;
                                memcpy(&np.second, &value, sizeof (T3));
                                memcpy(&np.first, &p2.first, sizeof (T1));
                                result->append(np);
                            }
                        }
                    }

                }
                delete iter1;
                delete iter2;
                //delete hashMap;
                return result;
            }

            template<class T1, class T2, class T3>
            static Bat<T1, T3>* col_fill(Bat<T1, T2> *arg, T3 value) {
                auto result = new TempBat<T1, T3>(arg->size());
                BatIterator<T1, T2> *iter = arg->begin();
                while (iter->hasNext()) {
                    pair<T1, T2> p = iter->next();
                    pair<T1, T3> np;
                    memcpy(&np.first, &p.first, sizeof (T1));
                    memcpy(&np.second, &value, sizeof (T3));
                    result->append(np);
                }
                delete iter;
                return result;
            }

            template<class T1, class T2>
            static Bat<T1, T2>* col_aggregate(Bat<T1, T2> *arg, T2 initValue) {
                auto result = new TempBat<T1, T2>();
                BatIterator<T1, T2> *iter = arg->begin();
                map<string, T2> *values = new map<string, T2>();
                map<string, T1> *tt = new map<string, T1>();
                while (iter->hasNext()) {
                    pair<T1, T2> p = iter->next();
                    string s = p.first.mS;
                    if (values->find(s) == values->end()) {
                        //	cout <<"not found"<<endl;
                        (*tt)[s] = p.first;
                        (*values)[s] = initValue;
                    } else {
                        (*values)[s] = (*values)[s] + (unsigned) 1;
                    }
                }
                delete iter;
                typedef typename std::map<string, T2>::iterator mapIter;
                for (mapIter m = values->begin(); m != values->end(); m++) {
                    T1 first = (*tt)[m->first];
                    T2 value = m->second;

                    string s = m->first;

                    cout << first << endl;
                    cout << value << endl;
                    //	result->append(make_pair(m->first, m->second));
                }
                delete values;
                return result;
            }

            template<class T1, class T2>
            static Bat<T1, T2>* col_aggregate_sum(Bat<T1, T2> *arg, T2 initValue) {
                auto result = new TempBat<T1, T2>();
                map<T1, T2> *values = new map<T1, T2>();
                BatIterator<T1, T2> *iter = arg->begin();
                while (iter->hasNext()) {
                    pair<T1, T2> p = iter->next();

                    if (values->find(p.first) == values->end()) {
                        //	cout <<"not found"<<endl;
                        (*values)[p.first] = initValue + p.second;
                    } else {
                        (*values)[p.first] = (*values)[p.first] + p.second;
                    }
                }
                delete iter;
                for (auto iter = values->begin(); iter != values->end(); iter++) {
                    //cout <<first<<endl;
                    //cout <<value<<endl;
                    result->append(make_pair(iter->first, iter->second));
                }
                delete values;
                return result;
            }

            /**
             * Simple sum operator
             * "valn = sum(val0..valn)"
             * @Author: burkhard
             **/
            template<class T1, class T2>
            static Bat<T1, T2>* sum_op(Bat<T1, T2> *arg) {
                auto result = new TempBat<T1, T2>(arg->size());
                BatIterator<T1, T2> *iter = arg->begin();
                T1 a = (T1) 0; //sum of p.first
                T2 b = (T2) 0; //sum of p.second
                while (iter->hasNext()) {
                    pair<T1, T2> p = iter->next();
                    a += p.first;
                    b += p.second;
                    result->append(make_pair(a, b));
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
            static Tail aggregate_sum(Bat<Head, Tail>* arg) {
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
            template<typename Result, typename Head1, typename Tail1, typename Head2, typename Tail2>
            static Result aggregate_mul_sum(Bat<Head1, Tail1>* arg1, Bat<Head2, Tail2>* arg2, Result init) {
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
            template<class T1, class T2>
            static Bat<T1, T2>* exp_sm_op(Bat<T1, T2> *arg, double alpha) {
                auto result = new TempBat<T1, T2>(arg->size());
                BatIterator<T1, T2> *iter = arg->begin();
                bool first = true; //the first element is added as it i.e. without being influenced by alpha
                pair<T1, T2> s; //last pair added to result
                while (iter->hasNext()) {
                    pair<T1, T2> p = iter->next(); //current pair
                    if (!first) s = make_pair((T1) (alpha * p.first + (1 - alpha) * s.first), (T2) (alpha * p.second + (1 - alpha) * s.second));
                    else {
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
            template<class T1, class T2>
            static Bat<T1, T2>* find_wp_op(Bat<T1, T2> *arg, double tolerance = 0.5) {
                auto result = new TempBat<T1, T2>();
                BatIterator<T1, T2> *iter = arg->begin();
                if (!iter->hasNext()) return result; //without elements, no WPs
                pair<T1, T2> lastwp = iter->next();
                result->append(lastwp); //the first point is always a WP
                std::cout << "WP: " << lastwp.first << "|" << lastwp.second << std::endl;
                if (!iter->hasNext()) return result; //without a second element we cannot find more WPs
                pair<T1, T2> tmp = iter->next();
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
                result->append(tmp); //last element is always a WP
                return result;
            }

        }
    }
}

#endif
