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
 * File:   operatorsAN.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 9. August 2016, 13:12
 */

#ifndef OPERATORSAN_TCC
#define OPERATORSAN_TCC

#include <ColumnStore.h>
#include <column_storage/Bat.h>
#include <column_storage/TempBat.h>
#include <util/resilience.hpp>

namespace v2 {
    namespace bat {
        namespace ops {

            template<typename Head, typename Tail>
            static Bat<Head, resint_t>* encodeA(Bat<Head, Tail>* arg, uint64_t A = ::A, size_t start = 0, size_t size = 0) {
                auto result = new TempBat<Head, resint_t>(arg->size());
                auto iter = arg->begin();
                if (iter->hasNext()) {
                    auto next = iter->get(start);
                    result->append(make_pair(next.first, static_cast<resint_t> (next.second) * A));
                    if (size) {
                        for (size_t step = 1; step < size && iter->hasNext(); ++step) {
                            next = iter->next();
                            result->append(make_pair(next.first, static_cast<resint_t> (next.second) * A));
                        }
                    } else {
                        while (iter->hasNext()) {
                            next = iter->next();
                            result->append(make_pair(next.first, static_cast<resint_t> (next.second) * A));
                        }
                    }
                }
                delete iter;
                return result; // possibly empty
            }

            template<typename Head>
            static vector<bool>* checkA(Bat<Head, resint_t>* arg, resint_t aInv = ::A_INV, resint_t unEncMaxU = ::AN_UNENC_MAX_U, size_t start = 0, size_t size = 0) {
                auto result = new vector<bool>();
                result->reserve(arg->size());
                auto iter = arg->begin();
                if (iter->hasNext()) {
                    result->emplace_back((iter->get(start).second * aInv) <= unEncMaxU);
                    if (size) {
                        for (size_t step = 1; step < size && iter->hasNext(); ++step) {
                            result->emplace_back((iter->next().second * aInv) <= unEncMaxU);
                        }
                    } else {
                        while (iter->hasNext()) {
                            result->emplace_back((iter->next().second * aInv) <= unEncMaxU);
                        }
                    }
                }
                delete iter;
                return result; // possibly empty
            }

            template<typename Head, typename Tail>
            static Bat<Head, Tail>* decodeA(Bat<Head, resint_t>* arg, uint64_t aInv = ::A_INV, resint_t unEncMaxU = ::AN_UNENC_MAX_U, size_t start = 0, size_t size = 0) {
                auto result = new TempBat<Head, Tail>(arg->size());
                auto iter = arg->begin();
                if (iter->hasNext()) {
                    auto current = iter->get(start);
                    current.second *= aInv;
                    result->append(current);
                    if (size) {
                        for (size_t step = 1; step < size && iter->hasNext(); ++step) {
                            current = iter->next();
                            current.second *= aInv;
                            result->append(current);
                        }
                    } else {
                        while (iter->hasNext()) {
                            current = iter->next();
                            current.second *= aInv;
                            result->append(current);
                        }
                    }
                }
                delete iter;
                return result;
            }

            template<typename Head, typename Tail>
            static pair<Bat<Head, Tail>*, vector<bool>*> checkAndDecodeA(Bat<Head, resint_t>* arg, uint64_t aInv = ::A_INV, resint_t unEncMaxU = ::AN_UNENC_MAX_U, size_t start = 0, size_t size = 0) {
                size_t sizeBAT = arg->size();
                auto result = make_pair(new TempBat<Head, Tail>(sizeBAT), new vector<bool>());
                result.second->reserve(sizeBAT);
                auto iter = arg->begin();
                if (iter->hasNext()) {
                    auto current = iter->get(start);
                    current.second *= aInv;
                    result.first->append(current);
                    result.second->emplace_back(current.second <= unEncMaxU);
                    if (size) {
                        for (size_t step = 1; step < size && iter->hasNext(); ++step) {
                            current = iter->next();
                            current.second *= aInv;
                            result.first->append(current);
                            result.second->emplace_back(current.second <= unEncMaxU);
                        }
                    } else {
                        while (iter->hasNext()) {
                            current = iter->next();
                            current.second *= aInv;
                            result.first->append(current);
                            result.second->emplace_back(current.second <= unEncMaxU);
                        }
                    }
                }
                delete iter;
                return result;
            }

        }
    }
}

#endif /* OPERATORSAN_TCC */
