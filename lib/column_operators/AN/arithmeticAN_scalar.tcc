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
 * File:   arithmeticAN_scalar.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 22. June 2017, 12:51
 */
#ifndef LIB_COLUMN_OPERATORS_AN_ARITHMETIC_SCALAR_TCC_
#define LIB_COLUMN_OPERATORS_AN_ARITHMETIC_SCALAR_TCC_

#include <stdexcept>
#include <type_traits>

#include <column_storage/TempStorage.hpp>
#include <util/v2typeconversion.hpp>
#include "../miscellaneous.hpp"
#include <column_operators/ANbase.hpp>
#include "ANhelper.tcc"

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
                    struct arithmeticAN {
                        typedef typename Head1::type_t h1_t;
                        typedef typename Tail1::type_t t1_t;
                        typedef typename Head2::type_t h2_t;
                        typedef typename Tail2::type_t t2_t;
                        typedef typename Result::type_t result_t;
                        typedef std::tuple<BAT<v2_void_t, Result> *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *> result_tuple_t;
                        typedef ANhelper<Head1> H1helper;
                        typedef ANhelper<Tail1> T1helper;
                        typedef ANhelper<Head2> H2helper;
                        typedef ANhelper<Tail2> T2helper;

                        static result_tuple_t run(
                                BAT<Head1, Tail1> * bat1,
                                BAT<Head2, Tail2> * bat2,
                                result_t AResult,
                                result_t AResultInv,
                                resoid_t AOID) {
                            static_assert(!std::is_same<Tail1, v2_str_t>::value, "Tail1 must not be a string type!");
                            static_assert(!std::is_same<Tail2, v2_str_t>::value, "Tail2 must not be a string type!");
                            static_assert(std::is_base_of<v2_anencoded_t, Tail1>::value, "Tail1 must be AN-encoded!");
                            static_assert(std::is_base_of<v2_anencoded_t, Tail2>::value, "Tail2 must be AN-encoded!");
                            if (bat1->size() != bat2->size()) {
                                throw std::runtime_error(CONCAT("arithmetic: bat1->size() != bat2->size() (", __FILE__, "@" TOSTRING(__LINE__), ")"));
                            }
                            auto result = new TempBAT<v2_void_t, Result>(ColumnDescriptor<v2_void_t, void>(),
                                    ColumnDescriptor<Result>(ColumnMetaData(size_bytes<result_t>, AResult, AResultInv, Result::UNENC_MAX_U, Result::UNENC_MIN))); // apply meta data from first BAT
                            result->reserve(bat1->size());
                            auto vecH1 = H1helper::createIndicatorVector();
                            auto vecT1 = T1helper::createIndicatorVector();
                            auto vecH2 = H2helper::createIndicatorVector();
                            auto vecT2 = T2helper::createIndicatorVector();
                            auto AH1inv = H1helper::getIfEncoded(bat1->head.metaData.AN_Ainv);
                            auto H1unencMaxU = H1helper::getIfEncoded(bat1->head.metaData.AN_unencMaxU);
                            auto AT1inv = T1helper::getIfEncoded(bat1->tail.metaData.AN_Ainv);
                            auto T1unencMaxU = T1helper::getIfEncoded(bat1->tail.metaData.AN_unencMaxU);
                            auto AH2inv = H2helper::getIfEncoded(bat2->head.metaData.AN_Ainv);
                            auto H2unencMaxU = H2helper::getIfEncoded(bat2->head.metaData.AN_unencMaxU);
                            auto AT2inv = T2helper::getIfEncoded(bat2->tail.metaData.AN_Ainv);
                            auto T2unencMaxU = T2helper::getIfEncoded(bat2->tail.metaData.AN_unencMaxU);
                            auto iter1 = bat1->begin();
                            auto iter2 = bat2->begin();
                            for (oid_t pos = 0; iter1->hasNext(); ++*iter1, ++*iter2, ++pos) {
                                h1_t h1 = H1helper::mulIfEncoded(iter1->head(), AH1inv);
                                if (H1helper::isEncoded && (h1 > H1unencMaxU)) {
                                    vecH1->push_back(pos * AOID);
                                }
                                t1_t t1 = T1helper::mulIfEncoded(iter1->tail(), AT1inv);
                                if (T1helper::isEncoded && (t1 > T1unencMaxU)) {
                                    vecT1->push_back(pos * AOID);
                                }
                                h2_t h2 = H2helper::mulIfEncoded(iter2->head(), AH2inv);
                                if (H2helper::isEncoded && (h2 > H2unencMaxU)) {
                                    vecH2->push_back(pos * AOID);
                                }
                                t2_t t2 = T2helper::mulIfEncoded(iter2->tail(), AT2inv);
                                if (T2helper::isEncoded && (t2 > T2unencMaxU)) {
                                    vecT2->push_back(pos * AOID);
                                }
                                result->append(Op<void>()(static_cast<result_t>(static_cast<result_t>(t1) * AResult), static_cast<result_t>(static_cast<result_t>(t2) * AResult)));
                            }
                            delete iter1;
                            delete iter2;
                            return result_tuple_t(result, vecH1, vecT1, vecH2, vecT2);
                        }
                    };

                }

                template<template<typename > class Op, typename Result, typename Head1, typename Tail1, typename Head2, typename Tail2>
                std::tuple<BAT<v2_void_t, Result> *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *> arithmeticAN(
                        BAT<Head1, Tail1> * bat1,
                        BAT<Head2, Tail2> * bat2,
                        typename Result::type_t AResult,
                        typename Result::type_t AResultInv,
                        resoid_t AOID) {
                    return Private::arithmeticAN<Op, Result, Head1, Tail1, Head2, Tail2>::run(bat1, bat2, AResult, AResultInv, AOID);
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

#endif /* LIB_COLUMN_OPERATORS_AN_ARITHMETIC_SCALAR_TCC_ */
