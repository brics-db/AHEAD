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

#ifndef SELECT_SEQ_TCC
#define SELECT_SEQ_TCC

#include <type_traits>

#include <ColumnStore.h>
#include <column_storage/Bat.h>
#include <column_storage/TempBat.h>
#include <column_operators/SSE.hpp>
#include <column_operators/SSECMP.hpp>
#include <column_operators/Normal/miscellaneous.tcc>

namespace v2 {
    namespace bat {
        namespace ops {
            namespace Private {

                template<template<typename > class Op, typename Head, typename Tail>
                struct Selection1 {

                    typedef typename Tail::type_t tail_t;
                    typedef typename Head::v2_select_t head_select_t;
                    typedef typename Tail::v2_select_t tail_select_t;
                    typedef BAT<head_select_t, tail_select_t> bat_t;

                    static bat_t*
                    filter(BAT<Head, Tail>* arg, tail_t && th) {
                        auto result = skeleton<head_select_t, tail_select_t>(arg);
                        result->reserve(arg->size());
                        auto iter = arg->begin();
                        Op<typename Tail::v2_compare_t::type_t> op;
                        for (; iter->hasNext(); ++*iter) {
                            auto t = iter->tail();
                            if (op(t, th)) {
                                result->append(std::make_pair(iter->head(), t));
                            }
                        }
                        delete iter;
                        return result;
                    }
                };

                template<template<typename > class Op, typename Head>
                struct Selection1<Op, Head, v2_str_t> {

                    typedef typename Head::v2_select_t head_select_t;
                    typedef v2_str_t tail_select_t;
                    typedef BAT<head_select_t, tail_select_t> bat_t;

                    static bat_t*
                    filter(BAT<Head, v2_str_t>* arg, str_t && threshold) {
                        auto result = skeleton<head_select_t, tail_select_t>(arg);
                        result->reserve(arg->size());
                        auto iter = arg->begin();
                        Op<int> op;
                        for (; iter->hasNext(); ++*iter) {
                            auto t = iter->tail();
                            if (op(strcmp(t, threshold), 0)) {
                                result->append(std::make_pair(iter->head(), t));
                            }
                        }
                        return result;
                    }
                };

                template<template<typename > class Op1, template<typename > class Op2, typename Head, typename Tail>
                struct Selection2 {

                    typedef typename Tail::type_t tail_t;
                    typedef typename Head::v2_select_t head_select_t;
                    typedef typename Tail::v2_select_t tail_select_t;
                    typedef BAT<head_select_t, tail_select_t> bat_t;

                    static bat_t*
                    filter(BAT<Head, Tail>* arg, tail_t && th1, tail_t && th2) {
                        auto result = skeleton<head_select_t, tail_select_t>(arg);
                        result->reserve(arg->size());
                        auto iter = arg->begin();
                        Op1<typename Tail::v2_compare_t::type_t> op1;
                        Op2<typename Tail::v2_compare_t::type_t> op2;
                        for (; iter->hasNext(); ++*iter) {
                            auto t = iter->tail();
                            if (op1(t, th1) & op2(t, th2)) {
                                result->append(std::make_pair(iter->head(), t));
                            }
                        }
                        delete iter;
                        return result;
                    }
                };

                template<template<typename > class Op1, template<typename > class Op2, typename Head>
                struct Selection2<Op1, Op2, Head, v2_str_t> {

                    typedef typename Head::v2_select_t head_select_t;
                    typedef v2_str_t tail_select_t;
                    typedef BAT<head_select_t, tail_select_t> bat_t;

                    static bat_t*
                    filter(BAT<Head, v2_str_t>* arg, tail_select_t && th1, tail_select_t && th2) {
                        auto result = skeleton<head_select_t, tail_select_t>(arg);
                        result->reserve(arg->size());
                        auto iter = arg->begin();
                        Op1<int> op1;
                        Op2<int> op2;
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
        }
    }
}

#endif /* SELECT_SEQ_TCC */
