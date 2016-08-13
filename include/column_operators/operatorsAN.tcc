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

            template<typename Tail, typename TypeRes>
            Bat<oid_t, TypeRes>* encodeA(Bat<oid_t, Tail>* arg, TypeRes A, size_t start = 0, size_t size = 0) {
                auto result = new TempBat<oid_t, TypeRes>(arg->size());
                auto iter = arg->begin();
                if (iter->hasNext()) {
                    auto next = iter->get(start);
                    result->append(make_pair(std::move(next.first), std::move(static_cast<TypeRes> (next.second) * A)));
                    if (size) {
                        for (size_t step = 1; step < size && iter->hasNext(); ++step) {
                            next = iter->next();
                            result->append(make_pair(std::move(next.first), std::move(static_cast<TypeRes> (next.second) * A)));
                        }
                    } else {
                        while (iter->hasNext()) {
                            next = iter->next();
                            result->append(make_pair(std::move(next.first), std::move(static_cast<TypeRes> (next.second) * A)));
                        }
                    }
                }
                delete iter;
                return result; // possibly empty
            }

            template<typename Tail, typename TypeRes>
            vector<bool>* checkA(Bat<oid_t, Tail>* arg, TypeRes aInv, TypeRes unEncMaxU, size_t start = 0, size_t size = 0) {
                auto result = new vector<bool>();
                result->reserve(arg->size());
                auto iter = arg->begin();
                if (iter->hasNext()) {
                    result->emplace_back(std::move((static_cast<TypeRes> (iter->get(start).second) * aInv) <= unEncMaxU));
                    if (size) {
                        for (size_t step = 1; step < size && iter->hasNext(); ++step) {
                            result->emplace_back(std::move((static_cast<TypeRes> (iter->next().second) * aInv) <= unEncMaxU));
                        }
                    } else {
                        while (iter->hasNext()) {
                            result->emplace_back(std::move((static_cast<TypeRes> (iter->next().second) * aInv) <= unEncMaxU));
                        }
                    }
                }
                delete iter;
                return result; // possibly empty
            }

            template<typename Tail, typename TypeRes>
            Bat<oid_t, Tail>* decodeA(Bat<oid_t, TypeRes>* arg, TypeRes aInv, TypeRes unEncMaxU, size_t start = 0, size_t size = 0) {
                auto result = new TempBat<oid_t, Tail>(arg->size());
                auto iter = arg->begin();
                if (iter->hasNext()) {
                    auto current = iter->get(start);
                    result->append(make_pair(std::move(current.first), std::move(static_cast<Tail> (current.second * aInv))));
                    if (size) {
                        for (size_t step = 1; step < size && iter->hasNext(); ++step) {
                            current = iter->next();
                            result->append(make_pair(std::move(current.first), std::move(static_cast<Tail> (current.second * aInv))));
                        }
                    } else {
                        while (iter->hasNext()) {
                            current = iter->next();
                            result->append(make_pair(std::move(current.first), std::move(static_cast<Tail> (current.second * aInv))));
                        }
                    }
                }
                delete iter;
                return result;
            }

            template<typename Tail, typename TypeRes>
            pair<Bat<oid_t, Tail>*, vector<bool>*> checkAndDecodeA(Bat<oid_t, TypeRes>* arg, TypeRes aInv, TypeRes unEncMaxU, size_t start = 0, size_t size = 0) {
                size_t sizeBAT = arg->size();
                auto result = make_pair(new TempBat<oid_t, Tail>(sizeBAT), new vector<bool>());
                result.second->reserve(sizeBAT);
                auto iter = arg->begin();
                if (iter->hasNext()) {
                    auto current = iter->get(start);
                    Tail dec = current.second * aInv;
                    result.first->append(std::move(make_pair(std::move(current.first), std::move(dec))));
                    result.second->emplace_back(std::move(dec <= unEncMaxU));
                    if (size) {
                        for (size_t step = 1; step < size && iter->hasNext(); ++step) {
                            current = iter->next();
                            Tail dec = current.second * aInv;
                            result.first->append(std::move(make_pair(std::move(current.first), std::move(dec))));
                            result.second->emplace_back(std::move(dec <= unEncMaxU));
                        }
                    } else {
                        while (iter->hasNext()) {
                            current = iter->next();
                            Tail dec = current.second * aInv;
                            result.first->append(std::move(make_pair(std::move(current.first), std::move(dec))));
                            result.second->emplace_back(std::move(dec <= unEncMaxU));
                        }
                    }
                }
                delete iter;
                return result;
            }

            template<class TypeRes>
            Bat<oid_t, resoid_t>* mirrorA(Bat<oid_t, TypeRes> *arg, TypeRes A = TypeSelector<TypeRes>::A) {
                auto result = new TempBat<oid_t, resoid_t>(arg->size());
                auto iter = arg->begin();
                while (iter->hasNext()) {
                    pair<oid_t, TypeRes> p = iter->next();
                    result->append(std::move(make_pair(std::move(p.first), std::move(static_cast<resoid_t> (p.first) * A))));
                }
                delete iter;
                return result;
            }

            template<class TypeRes>
            pair<Bat<oid_t, TypeRes>*, vector<bool>*> selection_ltA(Bat<oid_t, TypeRes>* arg, TypeRes treshold, TypeRes aInv = TypeSelector<TypeRes>::A_INV, TypeRes unEncMaxU = TypeSelector<TypeRes>::A_UNENC_MAX_U) {
                size_t sizeBAT = arg->size();
                auto result = make_pair(new TempBat<oid_t, TypeRes>(), new vector<bool>);
                result.second->reserve(sizeBAT);
                auto iter = arg->begin();
                while (iter->hasNext()) {
                    pair<oid_t, TypeRes> p = iter->next();
                    result.second->emplace_back((p.second * aInv) <= unEncMaxU);
                    if (p.second < treshold) {
                        result.first->append(std::move(make_pair(std::move(p.first), std::move(p.second))));
                    }
                }

                delete iter;

                return result;
            }

            template<class TypeRes>
            pair<Bat<oid_t, TypeRes>*, vector<bool>*> selection_btA(Bat<oid_t, TypeRes>* arg, TypeRes start, TypeRes end, TypeRes aInv = TypeSelector<TypeRes>::A_INV, TypeRes unEncMaxU = TypeSelector<TypeRes>::A_UNENC_MAX_U) {
                size_t sizeBAT = arg->size();
                auto result = make_pair(new TempBat<oid_t, TypeRes>(), new vector<bool>);
                result.second->reserve(sizeBAT);
                auto iter = arg->begin();
                while (iter->hasNext()) {
                    pair<oid_t, TypeRes> p = iter->next();
                    result.second->emplace_back((p.second * aInv) <= unEncMaxU);
                    if (p.second <= end && p.second >= start) {
                        result.first->append(std::move(make_pair(std::move(p.first), std::move(p.second))));
                    }
                }

                delete iter;

                return result;
            }

        }
    }
}

#endif /* OPERATORSAN_TCC */
