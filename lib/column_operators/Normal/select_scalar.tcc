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
 * File:   select_seq.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 22. November 2016, 21:02
 *
 * File:   select_scalar.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Renamed on 28. June 2017, 09:41
 */

#ifndef SELECT_SEQ_TCC
#define SELECT_SEQ_TCC

#include <type_traits>

#include <column_storage/Storage.hpp>
#include "../miscellaneous.hpp"

#ifdef __GNUC__
#pragma GCC target "no-sse"
#else
#warning "Forcing scalar code is not yet implemented for this compiler"
#endif

namespace ahead {
    namespace bat {
        namespace ops {
            namespace scalar {
                namespace Private {

                    template<template<typename > class Op, typename Head, typename Tail>
                    struct Selection1 {
                    };

                    template<template<typename > class Op, typename Tail>
                    struct Selection1<Op, v2_void_t, Tail> {

                        typedef typename Tail::type_t tail_t;
                        typedef typename v2_void_t::v2_select_t head_select_t;
                        typedef typename Tail::v2_select_t tail_select_t;
                        typedef BAT<head_select_t, tail_select_t> bat_t;

                        static bat_t*
                        filter(
                                BAT<v2_void_t, Tail>* arg,
                                tail_t th) {
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

                    template<template<typename > class Op>
                    struct Selection1<Op, v2_void_t, v2_str_t> {

                        typedef typename v2_void_t::v2_select_t v2_head_select_t;
                        typedef typename v2_str_t::v2_select_t v2_tail_select_t;
                        typedef BAT<v2_head_select_t, v2_tail_select_t> result_t;

                        static result_t* filter(
                                BAT<v2_void_t, v2_str_t>* arg,
                                str_t threshold) {
                            auto result = skeleton<v2_head_select_t, v2_tail_select_t>(arg);
                            result->reserve(arg->size());
                            auto iter = arg->begin();
                            Op<int> op;
                            for (; iter->hasNext(); ++*iter) {
                                auto t = iter->tail();
                                if (op(strcmp(t, threshold), 0)) {
                                    result->append(std::make_pair(iter->head(), t));
                                }
                            }
                            delete iter;
                            return result;
                        }
                    };

                    template<template<typename > class Op1, template<typename > class Op2, template<typename > class OpCombine, typename Head, typename Tail>
                    struct Selection2 {
                    };

                    template<template<typename > class Op1, template<typename > class Op2, template<typename > class OpCombine, typename Tail>
                    struct Selection2<Op1, Op2, OpCombine, v2_void_t, Tail> {

                        typedef typename Tail::type_t tail_t;
                        typedef typename v2_void_t::v2_select_t v2_head_select_t;
                        typedef typename Tail::v2_select_t v2_tail_select_t;
                        typedef BAT<v2_head_select_t, v2_tail_select_t> result_t;

                        static result_t* filter(
                                BAT<v2_void_t, Tail>* arg,
                                tail_t th1,
                                tail_t th2) {
                            static_assert(std::is_base_of<ahead::functor, OpCombine<void>>::value, "OpCombine template parameter must be a functor (see include/column_operators/functors.hpp)");
                            auto result = skeleton<v2_head_select_t, v2_tail_select_t>(arg);
                            result->reserve(arg->size());
                            auto iter = arg->begin();
                            Op1<typename Tail::v2_compare_t::type_t> op1;
                            Op2<typename Tail::v2_compare_t::type_t> op2;
                            for (; iter->hasNext(); ++*iter) {
                                auto t = iter->tail();
                                if (OpCombine<void>()(op1(t, std::forward<tail_t>(th1)), op2(t, std::forward<tail_t>(th2)))) {
                                    result->append(std::make_pair(iter->head(), t));
                                }
                            }
                            delete iter;
                            return result;
                        }
                    };

                    template<template<typename > class Op1, template<typename > class Op2, template<typename > class OpCombine>
                    struct Selection2<Op1, Op2, OpCombine, v2_void_t, v2_str_t> {

                        typedef typename v2_void_t::v2_select_t v2_head_select_t;
                        typedef typename v2_str_t::v2_select_t v2_tail_select_t;
                        typedef typename v2_tail_select_t::type_t tail_select_t;
                        typedef BAT<v2_oid_t, v2_str_t> result_t;

                        static result_t* filter(
                                BAT<v2_void_t, v2_str_t>* arg,
                                tail_select_t th1,
                                tail_select_t th2) {
                            static_assert(std::is_base_of<ahead::functor, OpCombine<void>>::value, "OpCombine template parameter must be a functor (see include/column_operators/functors.hpp)");
                            auto result = skeleton<v2_head_select_t, v2_tail_select_t>(arg);
                            result->reserve(arg->size());
                            auto iter = arg->begin();
                            Op1<int> op1;
                            Op2<int> op2;
                            for (; iter->hasNext(); ++*iter) {
                                auto t = iter->tail();
                                if (OpCombine<void>()(op1(strcmp(t, std::forward<tail_select_t>(th1)), 0), op2(strcmp(t, std::forward<tail_select_t>(th2)), 0))) {
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
}

#ifdef __GNUC__
#pragma GCC target "sse4.2"
#else
#warning "Unforcing scalar code is not yet implemented for this compiler"
#endif

#endif /* SELECT_SEQ_TCC */
