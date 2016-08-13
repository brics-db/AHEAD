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

            template<typename V2Type>
            vector<bool>* checkA(typename TypeSelector<V2Type>::res_bat_t* arg, typename TypeSelector<V2Type>::res_t aInv = TypeSelector<V2Type>::A_INV, typename TypeSelector<V2Type>::res_t unEncMaxU = TypeSelector<V2Type>::A_UNENC_MAX_U, size_t start = 0, size_t size = 0) {
                typedef typename TypeSelector<V2Type>::res_t Tail;
                auto result = new vector<bool>();
                result->reserve(arg->size());
                auto iter = arg->begin();
                if (iter->hasNext()) {
                    result->emplace_back(std::move((static_cast<Tail> (iter->get(start).second) * aInv) <= unEncMaxU));
                    if (size) {
                        for (size_t step = 1; step < size && iter->hasNext(); ++step) {
                            result->emplace_back(std::move((static_cast<Tail> (iter->next().second) * aInv) <= unEncMaxU));
                        }
                    } else {
                        while (iter->hasNext()) {
                            result->emplace_back(std::move((static_cast<Tail> (iter->next().second) * aInv) <= unEncMaxU));
                        }
                    }
                }
                delete iter;
                return result; // possibly empty
            }

            template<typename V2Type>
            typename TypeSelector<V2Type>::bat_t* decodeA(typename TypeSelector<V2Type>::res_bat_t* arg, typename TypeSelector<V2Type>::res_t aInv = TypeSelector<V2Type>::A_INV, typename TypeSelector<V2Type>::res_t unEncMaxU = TypeSelector<V2Type>::A_UNENC_MAX_U, size_t start = 0, size_t size = 0) {
                typedef typename TypeSelector<V2Type>::base_t Tail;
                auto result = new typename TypeSelector<V2Type>::tmp_t(arg->size());
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

            template<typename V2Type>
            pair<typename TypeSelector<V2Type>::bat_t*, vector<bool>*> checkAndDecodeA(typename TypeSelector<V2Type>::res_bat_t* arg, typename TypeSelector<V2Type>::res_t aInv = TypeSelector<V2Type>::A_INV, typename TypeSelector<V2Type>::res_t unEncMaxU = TypeSelector<V2Type>::A_UNENC_MAX_U, size_t start = 0, size_t size = 0) {
                size_t sizeBAT = arg->size();
                auto result = make_pair(new typename TypeSelector<V2Type>::tmp_t(sizeBAT), new vector<bool>());
                result.second->reserve(sizeBAT);
                auto iter = arg->begin();
                if (iter->hasNext()) {
                    auto current = iter->get(start);
                    typename TypeSelector<V2Type>::res_t dec = current.second * aInv;
                    result.first->append(std::move(make_pair(std::move(current.first), std::move(static_cast<typename TypeSelector<V2Type>::base_t> (dec)))));
                    result.second->emplace_back(std::move(dec <= unEncMaxU));
                    if (size) {
                        for (size_t step = 1; step < size && iter->hasNext(); ++step) {
                            current = iter->next();
                            dec = current.second * aInv;
                            result.first->append(std::move(make_pair(std::move(current.first), std::move(static_cast<typename TypeSelector<V2Type>::base_t> (dec)))));
                            result.second->emplace_back(std::move(dec <= unEncMaxU));
                        }
                    } else {
                        while (iter->hasNext()) {
                            current = iter->next();
                            dec = current.second * aInv;
                            result.first->append(std::move(make_pair(std::move(current.first), std::move(static_cast<typename TypeSelector<V2Type>::base_t> (dec)))));
                            result.second->emplace_back(std::move(dec <= unEncMaxU));
                        }
                    }
                }
                delete iter;
                return result;
            }

            template<class V2Type>
            pair<typename TypeSelector<V2Type>::res_bat_t*, vector<bool>*> selection_ltA(typename TypeSelector<V2Type>::res_bat_t* arg, typename TypeSelector<V2Type>::res_t treshold, typename TypeSelector<V2Type>::res_t aInv = TypeSelector<V2Type>::A_INV, typename TypeSelector<V2Type>::res_t unEncMaxU = TypeSelector<V2Type>::A_UNENC_MAX_U) {
                size_t sizeBAT = arg->size();
                auto result = make_pair(new typename TypeSelector<V2Type>::res_tmp_t, new vector<bool>);
                result.second->reserve(sizeBAT);
                auto iter = arg->begin();
                while (iter->hasNext()) {
                    auto p = iter->next();
                    result.second->emplace_back((p.second * aInv) <= unEncMaxU);
                    if (p.second < treshold) {
                        result.first->append(std::move(make_pair(std::move(p.first), std::move(p.second))));
                    }
                }
                delete iter;
                return result;
            }

            template<class V2Type>
            pair<typename TypeSelector<V2Type>::res_bat_t*, vector<bool>*> selection_btA(typename TypeSelector<V2Type>::res_bat_t* arg, typename TypeSelector<V2Type>::res_t start, typename TypeSelector<V2Type>::res_t end, typename TypeSelector<V2Type>::res_t aInv = TypeSelector<V2Type>::A_INV, typename TypeSelector<V2Type>::res_t unEncMaxU = TypeSelector<V2Type>::A_UNENC_MAX_U) {
                size_t sizeBAT = arg->size();
                auto result = make_pair(new typename TypeSelector<V2Type>::res_tmp_t(), new vector<bool>);
                result.second->reserve(sizeBAT);
                auto iter = arg->begin();
                while (iter->hasNext()) {
                    auto p = iter->next();
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
