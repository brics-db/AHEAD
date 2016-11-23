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
 * Created on 22. November 2016, 16:22
 */

#ifndef SELECT_AN_TCC
#define SELECT_AN_TCC

#include <type_traits>

#include <util/resilience.hpp>

namespace v2 {
    namespace bat {
        namespace ops {

            namespace Private {

                template<typename Op, typename Head, typename Tail>
                struct SelectionAN1 {

                    pair<Bat<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t>*, vector<bool>*> operator()(Bat<Head, Tail>* arg, typename Tail::type_t&& threshold, typename Tail::type_t aInv = Tail::A_INV, typename Tail::type_t unEncMaxU = Tail::A_UNENC_MAX_U, typename TypeMap<Head>::v2_encoded_t::type_t AHead = TypeMap<Head>::v2_encoded_t::A) {
                        static_assert(is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                        static_assert(is_base_of<v2_anencoded_t, Tail>::value, "ResTail must be an AN-encoded type");

                        auto result = make_pair(new TempBat<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t > (), new vector<bool>(arg->size()));
                        auto iter = arg->begin();
                        Op op;
                        for (size_t i = 0; iter->hasNext(); ++*iter, ++i) {
                            auto t = iter->tail();
                            if ((t * aInv) <= unEncMaxU) {
                                (*result.second)[i] = true;
                            }
                            if (op(t, threshold)) {
                                result.first->append(make_pair(iter->head() * AHead, t));
                            }
                        }
                        delete iter;
                        return result;
                    }
                };

                template<typename Op, typename Head>
                struct SelectionAN1<Op, Head, v2_str_t> {

                    pair<Bat<typename TypeMap<Head>::v2_encoded_t::v2_select_t, v2_str_t>*, vector<bool>*> operator()(Bat<Head, v2_str_t>* arg, str_t&& threshold, typename TypeMap<Head>::v2_encoded_t::type_t AHead = TypeMap<Head>::v2_encoded_t::A) {
                        static_assert(is_base_of<v2_base_t, Head>::value, "Head must be a base type");

                        auto result = make_pair(new TempBat<typename TypeMap<Head>::v2_encoded_t::v2_select_t, v2_str_t > (), nullptr);
                        auto iter = arg->begin();
                        Op op;
                        for (; iter->hasNext(); ++*iter) {
                            auto t = iter->tail();
                            if (op(strcmp(t, threshold), 0)) {
                                result.first->append(make_pair(iter->head() * AHead, t));
                            }
                        }
                        delete iter;
                        return result;
                    }
                };

                template<typename Op1, typename Op2, typename Head, typename Tail>
                struct SelectionAN2 {

                    pair<Bat<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t>*, vector<bool>*> operator()(Bat<Head, Tail>* arg, typename Tail::type_t&& threshold1, typename Tail::type_t&& threshold2, typename Tail::type_t aInv = Tail::A_INV, typename Tail::type_t unEncMaxU = Tail::A_UNENC_MAX_U, typename TypeMap<Head>::v2_encoded_t::type_t AHead = TypeMap<Head>::v2_encoded_t::A) {
                        static_assert(is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                        static_assert(is_base_of<v2_anencoded_t, Tail>::value, "ResTail must be an AN-encoded type");

                        auto result = make_pair(new TempBat<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t > (), new vector<bool>(arg->size()));
                        auto iter = arg->begin();
                        Op1 op1;
                        Op2 op2;
                        for (size_t i = 0; iter->hasNext(); ++*iter, ++i) {
                            auto t = iter->tail();
                            if ((t * aInv) <= unEncMaxU) {
                                (*result.second)[i] = true;
                            }
                            if (op1(t, threshold1) && op2(t, threshold2)) {
                                result.first->append(make_pair(iter->head() * AHead, t));
                            }
                        }
                        delete iter;
                        return result;
                    }
                };

                template<typename Op1, typename Op2, typename Head>
                struct SelectionAN2<Op1, Op2, Head, v2_str_t> {

                    pair<Bat<typename TypeMap<Head>::v2_encoded_t::v2_select_t, v2_str_t>*, vector<bool>*> operator()(Bat<Head, v2_str_t>* arg, str_t&& threshold1, str_t&& threshold2, typename TypeMap<Head>::v2_encoded_t::type_t AHead = TypeMap<Head>::v2_encoded_t::A) {
                        static_assert(is_base_of<v2_base_t, Head>::value, "Head must be a base type");

                        auto result = make_pair(new TempBat<typename TypeMap<Head>::v2_encoded_t::v2_select_t, v2_str_t > (), new vector<bool>());
                        auto iter = arg->begin();
                        Op1 op1;
                        Op2 op2;
                        for (; iter->hasNext(); ++*iter) {
                            auto t = iter->tail();
                            if (op1(strcmp(t, threshold1), 0) && op2(strcmp(t, threshold2), 0)) {
                                result.first->append(make_pair(iter->head() * AHead, t));
                            }
                        }
                        delete iter;
                        return result;
                    }
                };
            }

            template<template<typename> class Op, typename Head, typename Tail>
            pair<Bat<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t>*, vector<bool>*> selectAN(Bat<Head, Tail>* arg, typename Tail::type_t&& threshold) {
                Private::SelectionAN1 < Op<typename Tail::v2_compare_t::type_t>, Head, Tail> impl;
                return impl(arg, std::forward<typename Tail::type_t > (threshold));
            }

            template<template<typename> class Op1 = greater_equal, template<typename> class Op2 = less_equal, typename Head, typename Tail>
            pair<Bat<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t>*, vector<bool>*> selectAN(Bat<Head, Tail>* arg, typename Tail::type_t&& threshold1, typename Tail::type_t&& threshold2) {
                Private::SelectionAN2 < Op1<typename Tail::v2_compare_t::type_t>, Op2<typename Tail::v2_compare_t::type_t>, Head, Tail> impl;
                return impl(arg, std::forward<typename Tail::type_t > (threshold1), std::forward<typename Tail::type_t > (threshold2));
            }
        }
    }
}

#endif /* SELECT_AN_TCC */
