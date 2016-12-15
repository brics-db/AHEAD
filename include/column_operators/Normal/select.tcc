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
                        result->reserve(1024 * 1024);
                        auto iter = arg->begin();
                        Op op;
                        for (; iter->hasNext(); ++*iter) {
                            auto t = iter->tail();
                            if (op(t, threshold)) {
                                result->append(make_pair(iter->head(), t));
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
                        result->reserve(1024 * 1024);
                        auto iter = arg->begin();
                        Op op;
                        for (; iter->hasNext(); ++*iter) {
                            auto t = iter->tail();
                            if (op(strcmp(t, threshold), 0)) {
                                result->append(make_pair(iter->head(), t));
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
                        result->reserve(1024 * 1024);
                        auto iter = arg->begin();
                        Op1 op1;
                        Op2 op2;
                        for (; iter->hasNext(); ++*iter) {
                            auto t = iter->tail();
                            if (op1(t, th1) && op2(t, th2)) {
                                result->append(make_pair(iter->head(), t));
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
                        result->reserve(1024 * 1024);
                        auto iter = arg->begin();
                        Op1 op1;
                        Op2 op2;
                        for (; iter->hasNext(); ++*iter) {
                            auto t = iter->tail();
                            if (op1(strcmp(t, th1), 0) && op2(strcmp(t, th2), 0)) {
                                result->append(make_pair(iter->head(), t));
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
                return Private::Selection1 < Op<typename Tail::v2_compare_t::type_t>, Head, Tail > () (arg, move(th1));
            }

            template<template<typename> class Op1 = greater_equal, template<typename> class Op2 = less_equal, typename Head, typename Tail>
            BAT<typename Head::v2_select_t, typename Tail::v2_select_t>*
            select (BAT<Head, Tail>* arg, typename Tail::type_t && th1, typename Tail::type_t && th2) {
                return Private::Selection2 < Op1<typename Tail::type_t>, Op2<typename Tail::type_t>, Head, Tail > () (arg, move(th1), move(th2));
            }
        }
    }
}

#endif /* SELECT_TCC */
