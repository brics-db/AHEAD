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

#ifndef AGGREGATE_TCC
#define AGGREGATE_TCC

#include "aggregate_SSE.tcc"
#include "aggregate_seq.tcc"

namespace ahead {
    namespace bat {
        namespace ops {

            template<typename Result, typename Head, typename Tail>
            BAT<v2_void_t, Result> *
            aggregate_sum_grouped(
                    BAT<Head, Tail> * bat,
                    BAT<v2_void_t, v2_oid_t> * grouping,
                    size_t numGroups
                    ) {
                if (bat->size() != grouping->size()) {
                    throw std::runtime_error("bat and grouping must have the same size!");
                }
                typedef typename Result::type_t result_t;
                auto batResult = new TempBAT<v2_void_t, Result>();
                auto vec = batResult->tail.container.get();
                vec->resize(numGroups, result_t(0));
                auto iter = bat->begin();
                auto iterGroup = grouping->begin();
                for (; iter->hasNext(); ++*iter, ++*iterGroup) {
                    (*vec)[iterGroup->tail()] += iter->tail();
                }
                delete iter;
                delete iterGroup;
                return batResult;
            }

        }
    }
}

#endif /* AGGREGATE_TCC */
