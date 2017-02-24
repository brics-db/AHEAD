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
 * File:   select.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 22. November 2016, 21:02
 */

#ifndef SELECT_TCC
#define SELECT_TCC

#include <ColumnStore.h>
#include <column_storage/Bat.h>
#include <column_storage/TempBat.h>

namespace v2 {
    namespace bat {
        namespace ops {

            namespace Private {

                template<typename Op, typename Head, typename Tail>
                struct Selection1 {

                    typedef typename Tail::type_t tail_t;
                    typedef typename Head::v2_select_t head_select_t;
                    typedef typename Tail::v2_select_t tail_select_t;
                    typedef BAT<head_select_t, tail_select_t> bat_t;

                    bat_t* operator() (BAT<Head, Tail>* arg, tail_t && threshold) {
                        auto result = skeleton<head_select_t, tail_select_t>(arg);
                        result->reserve(arg->size() / 2);
                        auto iter = arg->begin();
                        Op op;
                        for (; iter->hasNext(); ++*iter) {
                            auto t = iter->tail();
                            if (op(t, threshold)) {
                                result->append(std::make_pair(iter->head(), t));
                            }
                        }
                        delete iter;
                        return result;
                    }
                };

                template<typename Op, typename Head>
                struct Selection1<Op, Head, v2_str_t> {

                    typedef typename Head::v2_select_t head_select_t;
                    typedef v2_str_t tail_select_t;
                    typedef BAT<head_select_t, tail_select_t> bat_t;

                    BAT<typename Head::v2_select_t, typename v2_str_t::v2_select_t>* operator() (BAT<Head, v2_str_t>* arg, str_t && threshold) {
                        auto result = skeleton<head_select_t, tail_select_t>(arg);
                        result->reserve(arg->size() / 2);
                        auto iter = arg->begin();
                        Op op;
                        for (; iter->hasNext(); ++*iter) {
                            auto t = iter->tail();
                            if (op(strcmp(t, threshold), 0)) {
                                result->append(std::make_pair(iter->head(), t));
                            }
                        }
                        return result;
                    }
                };

                template<typename Op1, typename Op2, typename Head, typename Tail>
                struct Selection2 {

                    typedef typename Tail::type_t tail_t;
                    typedef typename Head::v2_select_t head_select_t;
                    typedef typename Tail::v2_select_t tail_select_t;
                    typedef BAT<head_select_t, tail_select_t> bat_t;

                    BAT<typename Head::v2_select_t, typename Tail::v2_select_t>* operator() (BAT<Head, Tail>* arg, tail_t && th1, tail_t && th2) {
                        auto result = skeleton<head_select_t, tail_select_t>(arg);
                        result->reserve(arg->size() / 2);
                        auto iter = arg->begin();
                        Op1 op1;
                        Op2 op2;
                        for (; iter->hasNext(); ++*iter) {
                            auto t = iter->tail();
                            if (op1(t, th1) && op2(t, th2)) {
                                result->append(std::make_pair(iter->head(), t));
                            }
                        }
                        delete iter;
                        return result;
                    }
                };

                template<typename Op1, typename Op2, typename Head>
                struct Selection2<Op1, Op2, Head, v2_str_t> {

                    typedef typename Head::v2_select_t head_select_t;
                    typedef v2_str_t tail_select_t;
                    typedef BAT<head_select_t, tail_select_t> bat_t;

                    BAT<typename Head::v2_select_t, typename v2_str_t::v2_select_t>* operator() (BAT<Head, v2_str_t>* arg, str_t && th1, str_t && th2) {
                        auto result = skeleton<head_select_t, tail_select_t>(arg);
                        result->reserve(arg->size() / 2);
                        auto iter = arg->begin();
                        Op1 op1;
                        Op2 op2;
                        for (; iter->hasNext(); ++*iter) {
                            auto t = iter->tail();
                            if (op1(strcmp(t, th1), 0) && op2(strcmp(t, th2), 0)) {
                                result->append(std::make_pair(iter->head(), t));
                            }
                        }
                        delete iter;
                        return result;
                    }
                };
            }

            template<template <typename> class Op, typename Head, typename Tail>
            BAT<typename Head::v2_select_t, typename Tail::v2_select_t>*
            select (BAT<Head, Tail>* arg, typename Tail::type_t && th1) {
                return Private::Selection1 < Op<typename Tail::v2_compare_t::type_t>, Head, Tail > () (arg, std::move(th1));
            }

            template<template<typename> class Op1 = std::greater_equal, template<typename> class Op2 = std::less_equal, typename Head, typename Tail>
            BAT<typename Head::v2_select_t, typename Tail::v2_select_t>*
            select (BAT<Head, Tail>* arg, typename Tail::type_t && th1, typename Tail::type_t && th2) {
                return Private::Selection2 < Op1<typename Tail::type_t>, Op2<typename Tail::type_t>, Head, Tail > () (arg, std::move(th1), std::move(th2));
            }
        }
    }
}

#endif /* SELECT_TCC */
