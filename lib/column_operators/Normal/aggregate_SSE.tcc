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

#ifndef AGGREGATE_SSE_TCC
#define AGGREGATE_SSE_TCC

#include <column_storage/Storage.hpp>
#include "../SSE.hpp"
#include "../miscellaneous.hpp"

namespace ahead {
    namespace bat {
        namespace ops {
            namespace sse {

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
                template<typename Result, typename Head1, typename Tail1, typename Head2, typename Tail2>
                BAT<v2_void_t, Result>*
                aggregate_mul_sum(BAT<Head1, Tail1>* arg1, BAT<Head2, Tail2>* arg2, typename Result::type_t init = typename Result::type_t(0)) {
                    typedef typename Tail1::type_t tail1_t;
                    typedef typename Tail2::type_t tail2_t;
                    typedef typename Result::type_t result_t;
                    oid_t szTail1 = arg1->tail.container->size();
                    oid_t szTail2 = arg2->tail.container->size();
                    auto pT1 = arg1->tail.container->data();
                    auto pT1End = pT1 + szTail1;
                    auto pmmT1 = reinterpret_cast<__m128i *>(pT1);
                    auto pmmT1End = reinterpret_cast<__m128i *>(pT1End);
                    auto pT2 = arg2->tail.container->data();
                    auto pT2End = pT2 + szTail2;
                    auto pmmT2 = reinterpret_cast<__m128i *>(pT2);
                    auto pmmT2End = reinterpret_cast<__m128i *>(pT2End);
                    auto mmTotal = v2_mm128<result_t>::set1(0);
                    size_t inc1 = 0, inc2 = 0;
                    for (; (pmmT1 <= (pmmT1End - 1)) && (pmmT2 <= (pmmT2End - 1)); pmmT1 += inc1, pmmT2 += inc2) {
                        mmTotal = v2_mm128<result_t>::add(mmTotal, v2_mm128_mul_add<tail1_t, tail2_t, result_t>(pmmT1, pmmT2, inc1, inc2));
                    }
                    result_t total = init + v2_mm128<result_t>::sum(mmTotal);
                    pT1 = reinterpret_cast<tail1_t*>(pmmT1);
                    pT2 = reinterpret_cast<tail2_t*>(pmmT2);
                    for (; pT1 < pT1End && pT2 < pT2End; ++pT1, ++pT2) {
                        total += *pT1 * *pT2;
                    }
                    auto bat = new TempBAT<v2_void_t, Result>;
                    bat->append(total);
                    return bat;
                }

            }
        }
    }
}

#endif /* AGGREGATE_SSE_TCC */
