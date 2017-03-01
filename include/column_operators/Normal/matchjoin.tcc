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
 * File:   matchjoin.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 3. Januar 2017, 00:10
 */

#ifndef MATCHJOIN_TCC
#define MATCHJOIN_TCC

#include <limits>

#include <ColumnStore.h>
#include <column_storage/Bat.h>
#include <column_storage/TempBat.h>

namespace v2 {
    namespace bat {
        namespace ops {

            namespace Private {

                template<typename H1, typename T1, typename H2, typename T2>
                struct Matchjoin {

                    BAT<typename H1::v2_select_t, typename T2::v2_select_t>* operator()(BAT<H1, T1> *arg1, BAT<H2, T2> *arg2) {
                        auto result = skeletonJoin<typename H1::v2_select_t, typename T2::v2_select_t>(arg1, arg2);
                        result->reserve(arg1->size() < arg2->size() ? arg1->size() : arg2->size());
                        auto iter1 = arg1->begin();
                        auto iter2 = arg2->begin();
                        while (iter1->hasNext() && iter2->hasNext()) {
                            auto h2 = iter2->head();
                            auto t1 = iter1->tail();
                            for (; iter1->hasNext() && t1 < h2; ++*iter1, t1 = iter1->tail()) {
                            }
                            for (; iter2->hasNext() && t1 > h2; ++*iter2, h2 = iter2->head()) {
                            }
                            if (t1 == h2) {
                                result->append(std::make_pair(std::move(iter1->head()), std::move(iter2->tail())));
                                ++*iter1;
                                ++*iter2;
                            }
                        }
                        delete iter1;
                        delete iter2;
                        return result;
                    }
                };

                template<typename H1, typename T2>
                struct Matchjoin<H1, v2_void_t, v2_void_t, T2> {

                    BAT<typename H1::v2_select_t, typename T2::v2_select_t>* operator()(BAT<H1, v2_void_t> *arg1, BAT<v2_void_t, T2> *arg2) {
                        auto result = skeletonJoin<typename H1::v2_select_t, typename T2::v2_select_t>(arg1, arg2);
                        result->reserve(arg1->size() < arg2->size() ? arg1->size() : arg2->size());
                        auto iter1 = arg1->begin();
                        auto iter2 = arg2->begin();
                        if (iter1->hasNext() && iter2->hasNext()) {
                            if (arg1->tail.metaData.seqbase < arg2->head.metaData.seqbase) {
                                for (auto x = arg2->head.metaData.seqbase; iter1->hasNext() && iter2->hasNext() && iter1->tail() < x; ++*iter1) {
                                }
                            } else if (arg1->tail.metaData.seqbase > arg2->head.metaData.seqbase) {
                                for (auto x = arg1->tail.metaData.seqbase; iter1->hasNext() && iter2->hasNext() && iter2->head() < x; ++*iter2) {
                                }
                            }
                            for (; iter1->hasNext() && iter2->hasNext(); ++*iter1, ++*iter2) {
                                result->append(std::make_pair(std::move(iter1->head()), std::move(iter2->tail())));
                            }
                        }
                        delete iter1;
                        delete iter2;
                        return result;
                    }
                };
            }

            template<typename H1, typename T1, typename H2, typename T2>
            BAT<typename H1::v2_select_t, typename T2::v2_select_t>*
            matchjoin(BAT<H1, T1> *arg1, BAT<H2, T2> *arg2) {
                auto impl = Private::Matchjoin<H1, T1, H2, T2>();
                return impl(arg1, arg2);
            }

        }
    }
}

#endif /* MATCHJOIN_TCC */
