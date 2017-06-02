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
 * File:   aggregate.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 23. November 2016, 00:28
 */

#ifndef AGGREGATE_SEQ_TCC
#define AGGREGATE_SEQ_TCC

#include <column_storage/Storage.hpp>
#include "../miscellaneous.hpp"

namespace ahead {
    namespace bat {
        namespace ops {
            namespace scalar {

                /**
                 * Simple sum of all tail values in a Bat
                 * @param arg a Bat
                 * @return a single sum value
                 */
                template<typename v2_result_t, typename Head, typename Tail>
                typename v2_result_t::type_t
                aggregate_sum(BAT<Head, Tail>* arg) {
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
                template<typename V2Result, typename Head1, typename Tail1, typename Head2, typename Tail2>
                BAT<v2_void_t, V2Result> *
                aggregate_mul_sum(BAT<Head1, Tail1>* arg1, BAT<Head2, Tail2>* arg2, typename V2Result::type_t init = typename V2Result::type_t(0)) {
                    typedef typename V2Result::type_t result_t;
                    auto iter1 = arg1->begin();
                    auto iter2 = arg2->begin();
                    result_t total = init;
                    for (; iter1->hasNext() && iter2->hasNext(); ++*iter1, ++*iter2) {
                        total += (static_cast<result_t>(iter1->tail()) * static_cast<result_t>(iter2->tail()));
                    }
                    delete iter2;
                    delete iter1;
                    auto bat = new TempBAT<v2_void_t, V2Result>;
                    bat->append(total);
                    return bat;
                }

            }
        }
    }
}

#endif /* AGGREGATE_SEQ_TCC */
