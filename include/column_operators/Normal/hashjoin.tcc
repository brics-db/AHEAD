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
 * File:   hashjoin.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 23. November 2016, 00:25
 */

#ifndef HASHJOIN_TCC
#define HASHJOIN_TCC

#include <google/dense_hash_map>

#include <ColumnStore.h>
#include <column_storage/Bat.h>
#include <column_storage/TempBat.h>

#include "miscellaneous.tcc"

namespace v2 {
    namespace bat {
        namespace ops {

            template<typename H1, typename T1, typename H2, typename T2>
            BAT<typename H1::v2_select_t, typename T2::v2_select_t>*
            hashjoin (
                      BAT<H1, T1> *arg1,
                      BAT<H2, T2> *arg2,
                      hash_side_t side = hash_side_t::right
                      ) {
                auto result = skeletonJoin<typename H1::v2_select_t, typename T2::v2_select_t > (arg1, arg2);
                auto iter1 = arg1->begin();
                auto iter2 = arg2->begin();
                if (iter1->hasNext() && iter2->hasNext()) {
                    if (side == hash_side_t::left) {
                        google::dense_hash_map<typename T1::type_t, vector<typename H1::type_t >> hashMap;
                        hashMap.set_empty_key(T1::dhm_emptykey);
                        // std::unordered_map<typename T1::type_t, vector<typename H1::type_t >> hashMap;
                        for (; iter1->hasNext(); ++*iter1) {
                            hashMap[iter1->tail()].push_back(iter1->head());
                        }
                        auto mapEnd = hashMap.end();
                        for (; iter2->hasNext(); ++*iter2) {
                            auto iterMap = hashMap.find(iter2->head());
                            if (iterMap != mapEnd) {
                                auto t2 = iter2->tail();
                                for (auto matched : iterMap->second) {
                                    result->append(make_pair(matched, t2));
                                }
                            }
                        }
                    } else {
                        google::dense_hash_map<typename H2::type_t, vector<typename T2::type_t> > hashMap;
                        hashMap.set_empty_key(H2::dhm_emptykey);
                        // std::unordered_map<typename H2::type_t, vector<typename T2::type_t >> hashMap;
                        for (; iter2->hasNext(); ++*iter2) {
                            hashMap[iter2->head()].push_back(iter2->tail());
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
        }
    }
}

#endif /* HASHJOIN_TCC */
