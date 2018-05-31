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
 * File:   arithmetic_scalar.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 14-06-2017 15:54
 */
#ifndef LIB_COLUMN_OPERATORS_NORMAL_ARITHMETIC_SCALAR_TCC_
#define LIB_COLUMN_OPERATORS_NORMAL_ARITHMETIC_SCALAR_TCC_

#include <stdexcept>
#include <type_traits>

#include <ColumnStore.h>
#include <column_storage/Storage.hpp>
#include "../miscellaneous.hpp"

#ifdef __GNUC__
#pragma GCC push_options
#pragma GCC target "no-sse"
#else
#warning "Forcing scalar code is not yet implemented for this compiler"
#endif

namespace ahead {
    namespace bat {
        namespace ops {
            namespace scalar {

                namespace Private {

                    template<template<typename > class Op, typename Result, typename Head1, typename Tail1, typename Head2, typename Tail2>
                    struct arithmetic {

                        static BAT<v2_void_t, Result> *
                        run(
                                BAT<Head1, Tail1> * bat1,
                                BAT<Head2, Tail2> * bat2) {
                            static_assert(!std::is_same<Tail1, v2_str_t>::value, "Tail1 must not be a string type!");
                            static_assert(!std::is_same<Tail2, v2_str_t>::value, "Tail2 must not be a string type!");
                            if (bat1->size() != bat2->size()) {
                                throw std::runtime_error("arithmetic: bat1->size() != bat2->size()");
                            }
                            auto result = skeleton<v2_void_t, Result>(bat1); // apply meta data from first BAT
                            result->reserve(bat1->size());
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

#ifdef __GNUC__
#pragma GCC pop_options
#else
#warning "Unforcing scalar code is not yet implemented for this compiler"
#endif

#endif /* LIB_COLUMN_OPERATORS_NORMAL_ARITHMETIC_SCALAR_TCC_ */
