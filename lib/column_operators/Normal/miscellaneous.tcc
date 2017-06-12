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
 * File:   miscellaneous.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 22. November 2016, 21:00
 */

#ifndef MISCELLANEOUS_TCC
#define MISCELLANEOUS_TCC

#include <column_storage/Storage.hpp>
#include "../miscellaneous.hpp"

namespace ahead {
    namespace bat {
        namespace ops {

            namespace Private {

                template<typename Head, typename Tail>
                struct copy {

                    typedef typename Head::v2_copy_t CHead;
                    typedef typename Tail::v2_copy_t CTail;

                    static TempBAT<CHead, CTail> *
                    run(
                            BAT<Head, Tail> * arg) {
                        auto result = skeleton<CHead, CTail>(arg);
                        result->reserve(arg->size());
                        auto *iter = arg->begin();
                        for (; iter->hasNext(); ++*iter) {
                            result->append(std::make_pair(iter->head(), iter->tail()));
                        }
                        delete iter;
                        return result;
                    }
                };

                template<typename Tail>
                struct copy<v2_void_t, Tail> {

                    typedef typename Tail::v2_copy_t CTail;

                    static TempBAT<v2_void_t, CTail> *
                    run(
                            BAT<v2_void_t, Tail> * arg) {
                        auto result = skeleton<v2_void_t, CTail>(arg);
                        result->reserve(arg->size());
                        auto *iter = arg->begin();
                        for (; iter->hasNext(); ++*iter) {
                            result->append(iter->tail());
                        }
                        delete iter;
                        return result;
                    }
                };

            }

            template<typename Head, typename Tail>
            TempBAT<typename Head::v2_copy_t, typename Tail::v2_copy_t>*
            copy(
                    BAT<Head, Tail>* arg) {
                return Private::copy<typename Head::v2_copy_t, typename Tail::v2_copy_t>::run(arg);
            }
        }
    }
}

#endif /* MISCELLANEOUS_TCC */
