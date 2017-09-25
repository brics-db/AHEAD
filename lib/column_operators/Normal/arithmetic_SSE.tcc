// Copyright (c) 2017 Till Kolditz
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
 * File:   arithmetic_sse.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 14-06-2017 16:43
 */
#ifndef LIB_COLUMN_OPERATORS_NORMAL_ARITHMETIC_SSE_TCC_
#define LIB_COLUMN_OPERATORS_NORMAL_ARITHMETIC_SSE_TCC_

#include <stdexcept>
#include <type_traits>

#include <ColumnStore.h>
#include <column_storage/Storage.hpp>
#include "../miscellaneous.hpp"
#include "../SIMD/SSE.hpp"

#ifdef __GNUC__
#pragma GCC target "sse4.2"
#else
#warning "Forcing SSE 4.2 code is not yet implemented for this compiler"
#endif

namespace ahead {
    namespace bat {
        namespace ops {
            namespace simd {
                namespace sse {

                    namespace Private {

                        template<template<typename > class Op, typename Result, typename Head1, typename Tail1, typename Head2, typename Tail2>
                        struct arithmetic {

                            typedef typename Result::type_t result_t;
                            typedef typename Tail1::type_t tail_t;

                            static BAT<v2_void_t, Result> *
                            run(
                                    BAT<Head1, Tail1> * bat1,
                                    BAT<Head2, Tail2> * bat2) {
                                static_assert(!std::is_same<Tail1, v2_str_t>::value, "Tail1 must not be a string type!");
                                static_assert(!std::is_same<Tail2, v2_str_t>::value, "Tail2 must not be a string type!");
                                static_assert(is_instance_of<Tail1, Tail2>::value, "Tail1 and Tail2 must be the same (currently)!");
                                static_assert(is_instance_of<Tail1, Result>::value, "Tail1 and Result must be the same (currently)!");
                                if (bat1->size() != bat2->size()) {
                                    throw std::runtime_error(CONCAT("arithmetic: bat1->size() != bat2->size() (", __FILE__, "@" TOSTRING(__LINE__), ")"));
                                }
                                oid_t numValues = bat1->tail.container->size();
                                auto result = skeleton<v2_void_t, Result>(bat1); // apply meta data from first BAT
                                result->reserve(numValues);
                                auto pT1 = bat1->tail.container->data();
                                auto pT1End = pT1 + numValues;
                                auto pmmT1 = reinterpret_cast<__m128i *>(pT1);
                                auto pmmT1End = reinterpret_cast<__m128i *>(pT1End);
                                auto pT2 = bat2->tail.container->data();
                                auto pmmT2 = reinterpret_cast<__m128i *>(pT2);
                                auto pR = result->tail.container->data();
                                auto pmmR = reinterpret_cast<__m128i *>(pR);
                                for (; pmmT1 <= (pmmT1End - 1); pmmT1++, pmmT2++, pmmR++) {
                                    _mm_storeu_si128(pmmR, v2_mm_op<__m128i, tail_t, Op>::compute(*pmmT1, *pmmT2));
                                }
                                auto iter1 = bat1->begin();
                                auto iter2 = bat2->begin();
                                for (; iter1->hasNext(); ++*iter1, ++*iter2) {
                                    result->append(Op<void>()(iter1->tail(), iter2->tail()));
                                }
                                delete iter1;
                                delete iter2;
                                return result;
                            }
                        };

                    }

                    template<template<typename > class Op, typename Result, typename Head1, typename Tail1, typename Head2, typename Tail2>
                    BAT<v2_void_t, Result> *
                    arithmetic(
                            BAT<Head1, Tail1> * bat1,
                            BAT<Head2, Tail2> * bat2) {
                        return Private::arithmetic<Op, Result, Head1, Tail1, Head2, Tail2>::run(bat1, bat2);
                    }

                }
            }
        }
    }
}

#endif /* LIB_COLUMN_OPERATORS_NORMAL_ARITHMETIC_SSE_TCC_ */
