// Copyright (c) 2016-2017 Till Kolditz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/* 
 * File:   hashjoin.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 23. November 2016, 00:25
 */

#ifndef HASHJOIN_TCC
#define HASHJOIN_TCC

#include <limits>

#include <google/dense_hash_map>

#include <column_storage/Storage.hpp>
#include <column_operators/Normal/miscellaneous.tcc>

namespace ahead {
    namespace bat {
        namespace ops {

            template<typename H1, typename T1, typename H2, typename T2>
            BAT<typename H1::v2_select_t, typename T2::v2_select_t>*
            hashjoin(BAT<H1, T1> *arg1, BAT<H2, T2> *arg2, hash_side_t side = hash_side_t::right) {
                typedef typename H1::type_t h1_t;
                typedef typename T1::type_t t1_t;
                typedef typename H2::type_t h2_t;
                typedef typename T2::type_t t2_t;
                auto result = skeletonJoin<typename H1::v2_select_t, typename T2::v2_select_t>(arg1, arg2);
                auto iter1 = arg1->begin();
                auto iter2 = arg2->begin();
                if (iter1->hasNext() && iter2->hasNext()) {
                    typedef typename ahead::larger_type<t1_t, h2_t>::type_t larger_t;
                    if (side == hash_side_t::left) {
                        const larger_t t1max = static_cast<larger_t>(std::numeric_limits<t1_t>::max());
                        google::dense_hash_map<t1_t, std::vector<h1_t>> hashMap(arg1->size());
                        hashMap.set_empty_key(T1::dhm_emptykey);
                        for (; iter1->hasNext(); ++*iter1) {
                            hashMap[iter1->tail()].push_back(iter1->head());
                        }
                        auto mapEnd = hashMap.end();
                        for (; iter2->hasNext(); ++*iter2) {
                            auto h2 = static_cast<larger_t>(iter2->head());
                            if (h2 <= t1max) {
                                auto mapIter = hashMap.find(static_cast<t1_t>(h2));
                                if (mapIter != mapEnd) {
                                    auto t2 = iter2->tail();
                                    for (auto matched : mapIter->second) {
                                        result->append(std::make_pair(std::move(matched), std::move(t2)));
                                    }
                                }
                            }
                        }
                    } else {
                        const larger_t h2max = static_cast<larger_t>(std::numeric_limits<h2_t>::max());
                        google::dense_hash_map<h2_t, std::vector<t2_t> > hashMap(arg2->size());
                        hashMap.set_empty_key(H2::dhm_emptykey);
                        for (; iter2->hasNext(); ++*iter2) {
                            hashMap[iter2->head()].push_back(iter2->tail());
                        }
                        auto mapEnd = hashMap.end();
                        for (; iter1->hasNext(); ++*iter1) {
                            auto t1 = static_cast<larger_t>(iter1->tail());
                            if (t1 <= h2max) {
                                auto iterMap = hashMap.find(static_cast<h2_t>(t1));
                                if (iterMap != mapEnd) {
                                    auto h1 = iter1->head();
                                    for (auto matched : iterMap->second) {
                                        result->append(std::make_pair(std::move(h1), std::move(matched)));
                                    }
                                }
                            }
                        }
                    }
                }
                delete iter1;
                delete iter2;
                return result;
            }

        }
    }
}

#endif /* HASHJOIN_TCC */
