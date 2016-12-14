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
 * File:   aggregate.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 23. November 2016, 00:28
 */

#ifndef AGGREGATE_TCC
#define AGGREGATE_TCC

#include <google/dense_hash_map>

#include <ColumnStore.h>
#include <column_storage/Bat.h>
#include <column_storage/TempBat.h>

namespace v2 {
    namespace bat {
        namespace ops {

            /**
             * Simple sum of all tail values in a Bat
             * @param arg a Bat
             * @return a single sum value
             */
            template<typename v2_result_t, typename Head, typename Tail>
            typename v2_result_t::type_t
            aggregate_sum (BAT<Head, Tail>* arg) {
                typedef typename v2_result_t::type_t result_t;
                result_t sum = 0;
                auto iter = arg->begin();
                for (; iter->hasNext(); ++*iter) {
                    sum += static_cast<result_t>(iter->tail());
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
            Result
            aggregate_mul_sum (BAT<Head1, Tail1>* arg1, BAT<Head2, Tail2>* arg2, Result init = Result (0)) {
                auto iter1 = arg1->begin();
                auto iter2 = arg2->begin();
                Result total = init;
                for (; iter1->hasNext() && iter2->hasNext(); ++*iter1, ++*iter2) {
                    total += (static_cast<Result>(iter1->tail()) * static_cast<Result>(iter2->tail()));
                }
                delete iter2;
                delete iter1;
                return total;
            }

            namespace Private {

                /**
                 * Group-By the tail (base algorithm)
                 * Returns 2 BAT's:
                 * 1) Mapping (V)OID -> GroupID
                 * 2) GroupID -> Value
                 */
                template<typename Head, typename Tail>
                struct groupby_base {

                    template<typename HashMap>
                    pair<BAT<Head, v2_oid_t>*, BAT<v2_void_t, Tail>*>
                    group_base (BAT<Head, Tail>* bat, HashMap& dictionary) const {
                        typedef typename TempBAT<Head, v2_oid_t>::coldesc_head_t coldesc_head1_t;
                        typedef typename TempBAT<Head, v2_oid_t>::coldesc_tail_t coldesc_tail1_t;
                        typedef typename TempBAT<v2_void_t, Tail>::coldesc_head_t coldesc_head2_t;
                        typedef typename TempBAT<v2_void_t, Tail>::coldesc_tail_t coldesc_tail2_t;
                        auto batHeadtoGID = new TempBAT<Head, v2_oid_t>(coldesc_head1_t(bat->head.metaData), coldesc_tail1_t());
                        auto batGIDtoTail = new TempBAT<v2_void_t, Tail>(coldesc_head2_t(), coldesc_tail2_t(bat->tail.metaData));
                        auto iter = bat->begin();
                        oid_t nextGID = 0;
                        for (; iter->hasNext(); ++*iter) {
                            typename Tail::type_t curTail = iter->tail();
                            // search this tail in our mapping
                            // idx is the void value of mapGIDtoTail, which starts at zero
                            auto iterDict = dictionary.find(curTail);
                            if (iterDict == dictionary.end()) {
                                batGIDtoTail->append(curTail);
                                batHeadtoGID->append(make_pair(iter->head(), nextGID));
                                dictionary[curTail] = nextGID;
                                ++nextGID;
                            } else {
                                batHeadtoGID->append(make_pair(iter->head(), iterDict->second));
                            }
                        }
                        delete iter;
                        return make_pair(batHeadtoGID, batGIDtoTail);
                    }
                };

                /**
                 * Group By the tail (generic variant)
                 * Returns 2 BAT's:
                 * 1) Mapping (V)OID -> GroupID
                 * 2) GroupID -> Value
                 */
                template<typename Head, typename Tail>
                struct groupby : public groupby_base<Head, Tail> {

                    pair<BAT<Head, v2_oid_t>*, BAT<v2_void_t, Tail>*> operator() (BAT<Head, Tail>* bat) const {
                        google::dense_hash_map<typename Tail::type_t, oid_t> dictionary;
                        dictionary.set_empty_key(Tail::dhm_emptykey);
                        return this->group_base(bat, dictionary);
                    }
                };

                /**
                 * Group By the tail (string specialization)
                 * Returns 2 BAT's:
                 * 1) Mapping (V)OID -> GroupID
                 * 2) GroupID -> Value
                 */
                template<typename Head>
                struct groupby<Head, v2_str_t> : public groupby_base<Head, v2_str_t> {

                    pair<BAT<Head, v2_oid_t>*, BAT<v2_void_t, v2_str_t>*> operator() (BAT<Head, v2_str_t>* bat) const {
                        google::dense_hash_map<str_t, oid_t, hashstr, eqstr> dictionary;
                        dictionary.set_empty_key(v2_str_t::dhm_emptykey);
                        return this->group_base(bat, dictionary);
                    }
                };
            }

            /**
             * The actual Group By operator
             * @param bat
             * @return 2 BAT's:
             * 1) Mapping (V)OID -> GroupID
             * 2) GroupID -> Value
             */
            template<typename Head, typename Tail>
            pair<BAT<Head, v2_oid_t>*, BAT<v2_void_t, Tail>*>
            groupby (BAT<Head, Tail>* bat) {
                return Private::groupby<Head, Tail>()(bat);
            }

            /**
             * Group by 2 BAT's and sum up one column according to the double-grouping. The OID's (Heads) in all BAT's must match!
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
            tuple<BAT<v2_void_t, V2Result>*, BAT<v2_void_t, v2_oid_t>*, BAT<v2_void_t, Tail2>*, BAT<v2_void_t, v2_oid_t>*, BAT<v2_void_t, Tail3>*>
            groupedSum (BAT<Head1, Tail1>* bat1, BAT<Head2, Tail2>* bat2, BAT<Head3, Tail3>* bat3) {
#ifdef DEBUG
                StopWatch sw;
                sw.start();
#endif
                auto group1 = groupby(bat2);
#ifdef DEBUG
                auto time1 = sw.stop();
                sw.start();
#endif
                auto group2 = groupby(bat3);
#ifdef DEBUG
                auto time2 = sw.stop();
                sw.start();
#endif
                // create an array which can hold enough sums
                auto size1 = group1.second->size();
                auto size2 = group2.second->size();
                auto numgroups = size1 * size2;

                auto sumBat = new TempBAT<v2_void_t, V2Result>();
                sumBat->tail.container->resize(numgroups, 0);
                auto outBat2 = new TempBAT<v2_void_t, v2_oid_t>();
                outBat2->reserve(numgroups);
                auto outBat4 = new TempBAT<v2_void_t, v2_oid_t>();
                outBat4->reserve(numgroups);

                auto g1SecondIter = group1.second->begin();
                auto g2SecondIter = group2.second->begin();
                for (; g1SecondIter->hasNext(); ++*g1SecondIter) {
                    g2SecondIter->position(0);
                    for (; g2SecondIter->hasNext(); ++*g2SecondIter) {
                        outBat2->append(g1SecondIter->head());
                        outBat4->append(g2SecondIter->head());
                    }
                }

                auto sums = sumBat->tail.container->data();
                auto iter1 = bat1->begin();
                auto g1FirstIter = group1.first->begin();
                auto g2FirstIter = group2.first->begin();
#ifdef DEBUG
                auto iter2 = bat2->begin();
                auto iter3 = bat3->begin();
                std::cerr << "+------------+--------+-----------+\n";
                std::cerr << "| lo_revenue | d_year | p_brand   |\n";
                std::cerr << "+============+========+===========+\n";
                for (; iter1->hasNext(); ++*iter1, ++*iter2, ++*iter3, ++*g1FirstIter, ++*g2FirstIter) {
#else
                for (; iter1->hasNext(); ++*iter1, ++*g1FirstIter, ++*g2FirstIter) {
#endif
#ifdef DEBUG
                    std::cerr << "| " << std::setw(10) << iter1->tail();
                    std::cerr << " | " << std::setw(6) << iter2->tail();
                    std::cerr << " | " << std::setw(9) << iter3->tail() << " |\n";
                    // g1SecondIter->position(g1FirstIter->tail());
                    // std::cerr << std::setw(6) << g1SecondIter->tail() << " | ";
                    // g2SecondIter->position(g2FirstIter->tail());
                    // std::cerr << std::setw(9) << g2SecondIter->tail() << " |\n";
#endif
                    size_t pos = g1FirstIter->tail() * size2 + g2FirstIter->tail();
                    sums[pos] += static_cast<typename V2Result::type_t>(iter1->tail());
                }
#ifdef DEBUG
                delete iter3;
                delete iter2;
                std::cerr << "+------------+--------+-----------+";
#endif
                delete iter1;
                delete g1FirstIter;
                delete g2FirstIter;
                delete g1SecondIter;
                delete g2SecondIter;
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

#endif /* AGGREGATE_TCC */
