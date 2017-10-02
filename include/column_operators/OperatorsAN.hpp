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
 * File:   operatorsAN.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 9. August 2016, 13:12
 */

#ifndef OPERATORSAN_HPP
#define OPERATORSAN_HPP

#include <column_storage/TempStorage.hpp>
#include <column_operators/ANbase.hpp>
#include <column_operators/functors.hpp>

namespace ahead {
    namespace bat {
        namespace ops {

            template<typename H1, typename T1, typename H2, typename T2>
            std::tuple<BAT<typename H1::v2_select_t, typename T2::v2_select_t>*, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *>
            matchjoinAN(
                    BAT<H1, T1> * arg1,
                    BAT<H2, T2> * arg2,
                    resoid_t AOID = std::get<v2_resoid_t::AsBFW->size() - 1>(*v2_resoid_t::AsBFW));

            template<typename H1, typename T1, typename H2, typename T2>
            std::tuple<BAT<typename H1::v2_select_t, typename T2::v2_select_t>*, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *>
            matchjoinAN(
                    BAT<H1, T1> * arg1,
                    BAT<H2, T2> * arg2,
                    typename TypeMap<H1>::v2_encoded_t::type_t AH1reenc,
                    typename TypeMap<H1>::v2_encoded_t::type_t AH1InvReenc,
                    resoid_t AOID = std::get<v2_resoid_t::AsBFW->size() - 1>(*v2_resoid_t::AsBFW));

            template<typename H1, typename T1, typename H2, typename T2>
            std::tuple<BAT<typename H1::v2_select_t, typename T2::v2_select_t>*, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *>
            matchjoinAN(
                    BAT<H1, T1> * arg1,
                    BAT<H2, T2> * arg2,
                    typename TypeMap<H1>::v2_encoded_t::type_t AH1reenc,
                    typename TypeMap<H1>::v2_encoded_t::type_t AH1InvReenc,
                    typename TypeMap<T2>::v2_encoded_t::type_t AT2Reenc,
                    typename TypeMap<T2>::v2_encoded_t::type_t AT2InvReenc,
                    resoid_t AOID = std::get<v2_resoid_t::AsBFW->size() - 1>(*v2_resoid_t::AsBFW));

            template<typename Head1, typename Tail1, typename Head2, typename Tail2>
            std::tuple<TempBAT<typename Head1::v2_select_t, typename Tail2::v2_select_t>*, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *>
            hashjoinAN(
                    BAT<Head1, Tail1> * arg1,
                    BAT<Head2, Tail2> * arg2,
                    hash_side_t hashside = hash_side_t::right,
                    resoid_t AOID = std::get<v2_resoid_t::AsBFW->size() - 1>(*v2_resoid_t::AsBFW));

            template<typename Head1, typename Tail1, typename Head2, typename Tail2>
            std::tuple<TempBAT<typename TypeMap<Head1>::v2_encoded_t::v2_select_t, typename Tail2::v2_select_t>*, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *,
                    AN_indicator_vector *>
            hashjoinAN(
                    BAT<Head1, Tail1> * arg1,
                    BAT<Head2, Tail2> * arg2,
                    typename TypeMap<Head1>::v2_encoded_t::type_t AH1reenc,
                    typename TypeMap<Head1>::v2_encoded_t::type_t AH1InvReenc,
                    hash_side_t hashside = hash_side_t::right,
                    resoid_t AOID = std::get<v2_resoid_t::AsBFW->size() - 1>(*v2_resoid_t::AsBFW));

            template<typename Head1, typename Tail1, typename Head2, typename Tail2>
            std::tuple<TempBAT<typename TypeMap<Head1>::v2_encoded_t::v2_select_t, typename TypeMap<Tail2>::v2_encoded_t::v2_select_t>*, AN_indicator_vector *, AN_indicator_vector *,
                    AN_indicator_vector *, AN_indicator_vector *>
            hashjoinAN(
                    BAT<Head1, Tail1> * arg1,
                    BAT<Head2, Tail2> * arg2,
                    typename TypeMap<Head1>::v2_encoded_t::type_t AH1Reenc,
                    typename TypeMap<Head1>::v2_encoded_t::type_t AH1InvReenc,
                    typename TypeMap<Tail2>::v2_encoded_t::type_t AT2Reenc,
                    typename TypeMap<Tail2>::v2_encoded_t::type_t AT2InvReenc,
                    hash_side_t hashside = hash_side_t::right,
                    resoid_t AOID = std::get<v2_resoid_t::AsBFW->size() - 1>(*v2_resoid_t::AsBFW));

            template<typename T2>
            std::tuple<BAT<v2_void_t, typename TypeMap<T2>::v2_encoded_t> *, AN_indicator_vector *, AN_indicator_vector *>
            fetchjoinAN(
                    BAT<v2_void_t, v2_resoid_t> * arg1,
                    BAT<v2_void_t, T2> * arg2,
                    resoid_t AOID = std::get<v2_resoid_t::AsBFW->size() - 1>(*v2_resoid_t::AsBFW));

            template<typename T2>
            std::tuple<BAT<v2_void_t, typename TypeMap<T2>::v2_encoded_t> *, AN_indicator_vector *, AN_indicator_vector *>
            fetchjoinAN(
                    BAT<v2_void_t, v2_resoid_t> * arg1,
                    BAT<v2_void_t, T2> * arg2,
                    typename TypeMap<T2>::v2_encoded_t::type_t ATReenc,
                    typename TypeMap<T2>::v2_encoded_t::type_t ATReencInv,
                    resoid_t AOID = std::get<v2_resoid_t::AsBFW->size() - 1>(*v2_resoid_t::AsBFW));

            template<typename Head, typename Tail>
            std::tuple<BAT<v2_void_t, v2_resoid_t>*, BAT<v2_void_t, v2_resoid_t> *, AN_indicator_vector *, AN_indicator_vector *>
            groupbyAN(
                    BAT<Head, Tail>* bat,
                    typename v2_resoid_t::type_t AOID = std::get<v2_resoid_t::As->size() - 1>(*v2_resoid_t::As),
                    typename v2_resoid_t::type_t AOIDinv = std::get<v2_resoid_t::Ainvs->size() - 1>(*v2_resoid_t::Ainvs));

            template<typename Head, typename Tail>
            std::tuple<BAT<v2_void_t, v2_resoid_t>*, BAT<v2_void_t, v2_resoid_t> *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *>
            groupbyAN(
                    BAT<Head, Tail> * bat,
                    BAT<v2_void_t, v2_resoid_t> * grouping,
                    size_t numGroups,
                    typename v2_resoid_t::type_t AOID = std::get<v2_resoid_t::As->size() - 1>(*v2_resoid_t::As), // use largest A for encoding by default
                    typename v2_resoid_t::type_t AOIDinv = std::get<v2_resoid_t::Ainvs->size() - 1>(*v2_resoid_t::Ainvs) // and the appropriate inverse
                            );

            template<typename Result, typename Head, typename Tail>
            std::tuple<BAT<v2_void_t, Result> *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *>
            aggregate_sum_groupedAN(
                    BAT<Head, Tail> * bat,
                    BAT<v2_void_t, v2_resoid_t> * grouping,
                    size_t numGroups,
                    typename Result::type_t AResult = std::get<TypeMap<Result>::v2_encoded_t::As->size() - 1>(*TypeMap<Result>::v2_encoded_t::As), // use largest A for encoding by default
                    typename Result::type_t AResultInv = std::get<TypeMap<Result>::v2_encoded_t::Ainvs->size() - 1>(*TypeMap<Result>::v2_encoded_t::Ainvs),
                    resoid_t AOID = std::get<v2_resoid_t::As->size() - 1>(*v2_resoid_t::As) // use largest A for encoding by default
                            );

            namespace scalar {
                template<typename Head, typename Tail>
                BAT<Head, typename TypeMap<Tail>::v2_encoded_t>*
                encodeAN(
                        BAT<Head, Tail> * arg,
                        typename TypeMap<Tail>::v2_encoded_t::type_t A = std::get<Tail::As->size() - 1>(*Tail::As));

                template<typename Head, typename ResTail>
                AN_indicator_vector *
                checkAN(
                        BAT<Head, ResTail> * arg,
                        typename ResTail::type_t aInv = ResTail::A_INV,
                        typename ResTail::type_t unEncMaxU = ResTail::A_UNENC_MAX_U,
                        resoid_t AOID = std::get<v2_resoid_t::AsBFW->size() - 1>(*v2_resoid_t::AsBFW));

                template<typename Head, typename ResTail>
                std::pair<TempBAT<Head, typename ResTail::v2_unenc_t>*, AN_indicator_vector *>
                decodeAN(
                        BAT<Head, ResTail> * arg,
                        resoid_t AOID = std::get<v2_resoid_t::AsBFW->size() - 1>(*v2_resoid_t::AsBFW));

                template<typename Head, typename Tail>
                std::tuple<BAT<typename Head::v2_unenc_t, typename Tail::v2_unenc_t>*, AN_indicator_vector *, AN_indicator_vector *>
                checkAndDecodeAN(
                        BAT<Head, Tail> * arg,
                        resoid_t AOID = std::get<v2_resoid_t::AsBFW->size() - 1>(*v2_resoid_t::AsBFW));

                template<template<typename > class Op, typename Head, typename Tail>
                std::pair<BAT<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t>*, AN_indicator_vector *>
                selectAN(
                        BAT<Head, Tail> * arg,
                        typename Tail::type_t threshold,
                        resoid_t AOID = std::get<v2_resoid_t::AsBFW->size() - 1>(*v2_resoid_t::AsBFW));

                template<template<typename > class Op1, template<typename > class Op2, template<typename > class OpCombine, typename Head, typename Tail>
                std::pair<BAT<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t>*, AN_indicator_vector *>
                selectAN(
                        BAT<Head, Tail> * arg,
                        typename Tail::type_t threshold1,
                        typename Tail::type_t threshold2,
                        resoid_t AOID = std::get<v2_resoid_t::AsBFW->size() - 1>(*v2_resoid_t::AsBFW));

                template<template<typename > class Op, typename Head, typename Tail>
                std::pair<BAT<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t>*, AN_indicator_vector *>
                selectAN(
                        BAT<Head, Tail> * arg,
                        typename Tail::type_t threshold,
                        typename Tail::v2_select_t::type_t ATR,
                        typename Tail::v2_select_t::type_t ATInvR,
                        resoid_t AOID = std::get<v2_resoid_t::AsBFW->size() - 1>(*v2_resoid_t::AsBFW));

                template<template<typename > class Op1, template<typename > class Op2, template<typename > class OpCombine, typename Head, typename Tail>
                std::pair<BAT<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t>*, AN_indicator_vector *>
                selectAN(
                        BAT<Head, Tail> * arg,
                        typename Tail::type_t threshold1,
                        typename Tail::type_t threshold2,
                        typename Tail::v2_select_t::type_t ATR,
                        typename Tail::v2_select_t::type_t ATInvR,
                        resoid_t AOID = std::get<v2_resoid_t::AsBFW->size() - 1>(*v2_resoid_t::AsBFW));

                template<template<typename > class Op, typename Result, typename Head1, typename Tail1, typename Head2, typename Tail2>
                std::tuple<BAT<v2_void_t, Result> *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *>
                arithmeticAN(
                        BAT<Head1, Tail1> * bat1,
                        BAT<Head2, Tail2> * bat2,
                        typename Result::type_t AResult = std::get<Result::As->size() - 1>(*Result::As),
                        typename Result::type_t AResultInv = std::get<Result::Ainvs->size() - 1>(*Result::Ainvs),
                        resoid_t AOID = std::get<v2_resoid_t::AsBFW->size() - 1>(*v2_resoid_t::AsBFW));

                template<typename Result, typename Head1, typename Tail1, typename Head2, typename Tail2, typename ResEnc = typename TypeMap<Result>::v2_encoded_t, typename T1Enc = typename TypeMap<
                        Tail1>::v2_encoded_t, typename T2Enc = typename TypeMap<Tail2>::v2_encoded_t>
                std::tuple<BAT<v2_void_t, Result> *, AN_indicator_vector *, AN_indicator_vector *>
                aggregate_mul_sumAN(
                        BAT<Head1, Tail1> * arg1,
                        BAT<Head2, Tail2> * arg2,
                        typename Result::type_t init = typename Result::type_t(0),
                        typename ResEnc::type_t RA = std::get<ResEnc::As->size() - 1>(*ResEnc::As),
                        typename ResEnc::type_t RAInv = std::get<ResEnc::Ainvs->size() - 1>(*ResEnc::Ainvs),
                        resoid_t AOID = std::get<v2_resoid_t::AsBFW->size() - 1>(*v2_resoid_t::AsBFW));
            }

            namespace simd {
                namespace sse {
                    template<typename Head, typename Tail>
                    BAT<Head, typename TypeMap<Tail>::v2_encoded_t>*
                    encodeAN(
                            BAT<Head, Tail> * arg,
                            typename TypeMap<Tail>::v2_encoded_t::type_t A = std::get<Tail::As->size() - 1>(*Tail::As));

                    template<typename Head, typename ResTail>
                    AN_indicator_vector *
                    checkAN(
                            BAT<Head, ResTail> * arg,
                            typename ResTail::type_t aInv = ResTail::A_INV,
                            typename ResTail::type_t unEncMaxU = ResTail::A_UNENC_MAX_U,
                            resoid_t AOID = std::get<v2_resoid_t::AsBFW->size() - 1>(*v2_resoid_t::AsBFW));

                    template<typename Head, typename ResTail>
                    std::pair<TempBAT<Head, typename ResTail::v2_unenc_t>*, AN_indicator_vector *>
                    decodeAN(
                            BAT<Head, ResTail> * arg,
                            resoid_t AOID = std::get<v2_resoid_t::AsBFW->size() - 1>(*v2_resoid_t::AsBFW));

                    template<typename Head, typename Tail>
                    std::tuple<BAT<typename Head::v2_unenc_t, typename Tail::v2_unenc_t>*, AN_indicator_vector *, AN_indicator_vector *>
                    checkAndDecodeAN(
                            BAT<Head, Tail> * arg,
                            resoid_t AOID = std::get<v2_resoid_t::AsBFW->size() - 1>(*v2_resoid_t::AsBFW));

                    template<template<typename > class Op, typename Head, typename Tail>
                    std::pair<BAT<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t>*, AN_indicator_vector*>
                    selectAN(
                            BAT<Head, Tail> * arg,
                            typename Tail::type_t threshold,
                            resoid_t AOID = std::get<v2_resoid_t::AsBFW->size() - 1>(*v2_resoid_t::AsBFW));

                    template<template<typename > class Op1, template<typename > class Op2, template<typename > class OpCombine, typename Head, typename Tail>
                    std::pair<BAT<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t> *, AN_indicator_vector *>
                    selectAN(
                            BAT<Head, Tail> * arg,
                            typename Tail::type_t threshold1,
                            typename Tail::type_t threshold2,
                            resoid_t AOID = std::get<v2_resoid_t::AsBFW->size() - 1>(*v2_resoid_t::AsBFW));

                    template<template<typename > class Op, typename Head, typename Tail>
                    std::pair<BAT<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t> *, AN_indicator_vector *>
                    selectAN(
                            BAT<Head, Tail> * arg,
                            typename Tail::type_t threshold,
                            typename Tail::v2_select_t::type_t ATReenc,
                            typename Tail::v2_select_t::type_t ATReencInv,
                            resoid_t AOID = std::get<v2_resoid_t::AsBFW->size() - 1>(*v2_resoid_t::AsBFW));

                    template<template<typename > class Op1, template<typename > class Op2, template<typename > class OpCombine, typename Head, typename Tail>
                    std::pair<BAT<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t> *, AN_indicator_vector *>
                    selectAN(
                            BAT<Head, Tail> * arg,
                            typename Tail::type_t threshold1,
                            typename Tail::type_t threshold2,
                            typename Tail::v2_select_t::type_t ATReenc,
                            typename Tail::v2_select_t::type_t ATReencInv,
                            resoid_t AOID = std::get<v2_resoid_t::AsBFW->size() - 1>(*v2_resoid_t::AsBFW));

                    template<template<typename > class Op, typename Result, typename Head1, typename Tail1, typename Head2, typename Tail2>
                    std::tuple<BAT<v2_void_t, Result> *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *> arithmeticAN(
                            BAT<Head1, Tail1> * bat1,
                            BAT<Head2, Tail2> * bat2,
                            typename Result::type_t AResult = std::get<Result::As->size() - 1>(*Result::As),
                            typename Result::type_t AResultInv = std::get<Result::Ainvs->size() - 1>(*Result::Ainvs),
                            resoid_t AOID = std::get<v2_resoid_t::AsBFW->size() - 1>(*v2_resoid_t::AsBFW));

                    template<typename Result, typename Head1, typename Tail1, typename Head2, typename Tail2, typename ResEnc = typename TypeMap<Result>::v2_encoded_t,
                            typename T1Enc = typename TypeMap<Tail1>::v2_encoded_t, typename T2Enc = typename TypeMap<Tail2>::v2_encoded_t>
                    std::tuple<BAT<v2_void_t, Result>*, AN_indicator_vector *, AN_indicator_vector *>
                    aggregate_mul_sumAN(
                            BAT<Head1, Tail1> * arg1,
                            BAT<Head2, Tail2> * arg2,
                            typename Result::type_t init = typename Result::type_t(0),
                            typename ResEnc::type_t AResult = std::get<ResEnc::As->size() - 1>(*ResEnc::As),
                            typename ResEnc::type_t AResultInv = std::get<ResEnc::Ainvs->size() - 1>(*ResEnc::Ainvs),
                            resoid_t AOID = std::get<v2_resoid_t::AsBFW->size() - 1>(*v2_resoid_t::AsBFW));
                }
            }

        }
    }
}

#endif /* OPERATORSAN_HPP */
