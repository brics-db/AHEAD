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

#include <column_storage/Storage.hpp>
#include <column_operators/ANbase.hpp>

namespace ahead {
    namespace bat {
        namespace ops {

            template<typename H1, typename T1, typename H2, typename T2>
            std::tuple<TempBAT<typename H1::v2_select_t, typename T2::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*>
            matchjoinAN(
                    BAT<H1, T1> *arg1,
                    BAT<H2, T2> *arg2
                    );

            template<typename H1, typename T1, typename H2, typename T2>
            std::tuple<TempBAT<typename H1::v2_select_t, typename T2::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*>
            matchjoinAN(
                    BAT<H1, T1> *arg1,
                    BAT<H2, T2> *arg2,
                    typename TypeMap<H1>::v2_encoded_t::type_t AH1reenc,
                    typename TypeMap<H1>::v2_encoded_t::type_t AH1InvReenc
                    );

            template<typename H1, typename T1, typename H2, typename T2>
            std::tuple<TempBAT<typename H1::v2_select_t, typename T2::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*>
            matchjoinAN(
                    BAT<H1, T1> *arg1,
                    BAT<H2, T2> *arg2,
                    typename TypeMap<H1>::v2_encoded_t::type_t AH1reenc,
                    typename TypeMap<H1>::v2_encoded_t::type_t AH1InvReenc,
                    typename TypeMap<T1>::v2_encoded_t::type_t AT2Reenc,
                    typename TypeMap<T1>::v2_encoded_t::type_t AT2InvReenc
                    );

            template<typename Head1, typename Tail1, typename Head2, typename Tail2>
            std::tuple<TempBAT<typename Head1::v2_select_t, typename Tail2::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*>
            hashjoinAN(
                    BAT<Head1, Tail1>* arg1,
                    BAT<Head2, Tail2>* arg2,
                    hash_side_t hashside = hash_side_t::right
                    );

            template<typename Head1, typename Tail1, typename Head2, typename Tail2>
            std::tuple<TempBAT<typename TypeMap<Head1>::v2_encoded_t::v2_select_t, typename Tail2::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*>
            hashjoinAN(
                    BAT<Head1, Tail1>* arg1,
                    BAT<Head2, Tail2>* arg2,
                    typename TypeMap<Head1>::v2_encoded_t::type_t AH1reenc,
                    typename TypeMap<Head1>::v2_encoded_t::type_t AH1InvReenc,
                    hash_side_t hashside = hash_side_t::right
                    );

            template<typename Head1, typename Tail1, typename Head2, typename Tail2>
            std::tuple<TempBAT<typename TypeMap<Head1>::v2_encoded_t::v2_select_t, typename TypeMap<Tail2>::v2_encoded_t::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*,
                    std::vector<bool>*>
            hashjoinAN(
                    BAT<Head1, Tail1>* arg1,
                    BAT<Head2, Tail2>* arg2,
                    typename TypeMap<Head1>::v2_encoded_t::type_t AH1Reenc,
                    typename TypeMap<Head1>::v2_encoded_t::type_t AH1InvReenc,
                    typename TypeMap<Tail2>::v2_encoded_t::type_t AT2Reenc,
                    typename TypeMap<Tail2>::v2_encoded_t::type_t AT2InvReenc,
                    hash_side_t hashside = hash_side_t::right
                    );

            template<typename Head, typename Tail>
            std::tuple<BAT<Head, v2_resoid_t>*, BAT<v2_void_t, Tail>*, std::vector<bool>*, std::vector<bool>*>
            groupbyAN(
                    BAT<Head, Tail>* bat,
                    typename v2_resoid_t::type_t AOID = std::get<ANParametersSelector<v2_resoid_t>::As->size() - 1>(*ANParametersSelector<v2_resoid_t>::As),
                    typename v2_resoid_t::type_t AOIDinv = std::get<ANParametersSelector<v2_resoid_t>::Ainvs->size() - 1>(*ANParametersSelector<v2_resoid_t>::Ainvs)
                    );

            template<typename Head, typename Tail>
            std::tuple<BAT<v2_void_t, v2_resoid_t>*, BAT<v2_void_t, v2_resoid_t>*, std::vector<bool>*, std::vector<bool>*>
            groupbyAN(
                    BAT<Head, Tail> * bat,
                    BAT<v2_void_t, v2_resoid_t> * grouping,
                    typename v2_resoid_t::type_t AOID = std::get<ANParametersSelector<v2_resoid_t>::As->size() - 1>(*ANParametersSelector<v2_resoid_t>::As), // use largest A for encoding by default
                    typename v2_resoid_t::type_t AOIDinv = std::get<ANParametersSelector<v2_resoid_t>::Ainvs->size() - 1>(*ANParametersSelector<v2_resoid_t>::Ainvs) // and the appropriate inverse
                    );

            namespace scalar {

                template<typename Head, typename Tail>
                BAT<Head, typename TypeMap<Tail>::v2_encoded_t>*
                encodeAN(
                        BAT<Head, Tail>* arg,
                        typename TypeMap<Tail>::v2_encoded_t::type_t A = std::get<ANParametersSelector<Tail>::As->size() - 1>(*ANParametersSelector<Tail>::As)
                        );

                template<typename Head, typename ResTail>
                std::vector<bool>*
                checkAN(
                        BAT<Head, ResTail>* arg,
                        typename ResTail::type_t aInv = ResTail::A_INV, typename ResTail::type_t unEncMaxU = ResTail::A_UNENC_MAX_U
                        );

                template<typename Head, typename ResTail>
                std::pair<TempBAT<Head, typename ResTail::v2_unenc_t>*, std::vector<bool>*>
                decodeAN(
                        BAT<Head, ResTail>* arg
                        );

                template<typename Head, typename Tail>
                std::tuple<BAT<typename Head::v2_unenc_t, typename Tail::v2_unenc_t>*, std::vector<bool>*, std::vector<bool>*>
                checkAndDecodeAN(
                        BAT<Head, Tail>* arg
                        );

                template<template<typename > class Op, typename Head, typename Tail>
                std::pair<BAT<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t>*, AN_indicator_vector*>
                selectAN(
                        BAT<Head, Tail>* arg,
                        typename Tail::type_t&& threshold
                        );

                template<template<typename > class Op1 = std::greater_equal, template<typename > class Op2 = std::less_equal, typename Head, typename Tail>
                std::pair<BAT<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t>*, AN_indicator_vector*>
                selectAN(
                        BAT<Head, Tail>* arg,
                        typename Tail::type_t&& threshold1,
                        typename Tail::type_t&& threshold2
                        );

                template<template<typename > class Op, typename Head, typename Tail>
                std::pair<BAT<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t>*, AN_indicator_vector*>
                selectAN(
                        BAT<Head, Tail>* arg,
                        typename Tail::type_t && threshold,
                        typename Tail::v2_select_t::type_t ATR,
                        typename Tail::v2_select_t::type_t ATInvR
                        );

                template<template<typename > class Op1 = std::greater_equal, template<typename > class Op2 = std::less_equal, typename Head, typename Tail>
                std::pair<BAT<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t>*, AN_indicator_vector*>
                selectAN(
                        BAT<Head, Tail>* arg,
                        typename Tail::type_t&& threshold1,
                        typename Tail::type_t&& threshold2,
                        typename Tail::v2_select_t::type_t ATR,
                        typename Tail::v2_select_t::type_t ATInvR
                        );

                template<typename Result, typename Head1, typename Tail1, typename Head2, typename Tail2, typename ResEnc = typename TypeMap<Result>::v2_encoded_t, typename T1Enc = typename TypeMap<Tail1>::v2_encoded_t, typename T2Enc = typename TypeMap<Tail2>::v2_encoded_t>
                std::tuple<BAT<v2_void_t, Result>*, std::vector<bool>*, std::vector<bool>*>
                aggregate_mul_sumAN(
                        BAT<Head1, Tail1>* arg1,
                        BAT<Head2, Tail2>* arg2,
                        typename Result::type_t init = typename Result::type_t(0),
                        typename ResEnc::type_t RA = std::get<ANParametersSelector<ResEnc>::As->size() - 1>(*ANParametersSelector<ResEnc>::As),
                        typename ResEnc::type_t RAInv = std::get<ANParametersSelector<ResEnc>::Ainvs->size() - 1>(*ANParametersSelector<ResEnc>::Ainvs)
                        );

            }

            namespace sse {

                template<typename Head, typename Tail>
                BAT<Head, typename TypeMap<Tail>::v2_encoded_t>*
                encodeAN(
                        BAT<Head, Tail>* arg,
                        typename TypeMap<Tail>::v2_encoded_t::type_t A = std::get<ANParametersSelector<Tail>::As->size() - 1>(*ANParametersSelector<Tail>::As)
                        );

                template<typename Head, typename ResTail>
                std::vector<bool>*
                checkAN(
                        BAT<Head, ResTail>* arg,
                        typename ResTail::type_t aInv = ResTail::A_INV, typename ResTail::type_t unEncMaxU = ResTail::A_UNENC_MAX_U
                        );

                template<typename Head, typename ResTail>
                std::pair<TempBAT<Head, typename ResTail::v2_unenc_t>*, std::vector<bool>*>
                decodeAN(
                        BAT<Head, ResTail>* arg
                        );

                template<typename Head, typename Tail>
                std::tuple<BAT<typename Head::v2_unenc_t, typename Tail::v2_unenc_t>*, std::vector<bool>*, std::vector<bool>*>
                checkAndDecodeAN(
                        BAT<Head, Tail>* arg
                        );

                template<template<typename > class Op, typename Head, typename Tail>
                std::pair<BAT<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t>*, AN_indicator_vector*>
                selectAN(
                        BAT<Head, Tail>* arg,
                        typename Tail::type_t&& threshold
                        );

                template<template<typename > class Op1 = std::greater_equal, template<typename > class Op2 = std::less_equal, typename Head, typename Tail>
                std::pair<BAT<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t>*, AN_indicator_vector*>
                selectAN(
                        BAT<Head, Tail>* arg,
                        typename Tail::type_t&& threshold1,
                        typename Tail::type_t&& threshold2
                        );

                template<template<typename > class Op, typename Head, typename Tail>
                std::pair<BAT<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t>*, AN_indicator_vector*>
                selectAN(
                        BAT<Head, Tail>* arg,
                        typename Tail::type_t && threshold,
                        typename Tail::v2_select_t::type_t ATR,
                        typename Tail::v2_select_t::type_t ATInvR
                        );

                template<template<typename > class Op1 = std::greater_equal, template<typename > class Op2 = std::less_equal, typename Head, typename Tail>
                std::pair<BAT<typename TypeMap<Head>::v2_encoded_t::v2_select_t, typename Tail::v2_select_t>*, AN_indicator_vector*>
                selectAN(
                        BAT<Head, Tail>* arg,
                        typename Tail::type_t&& threshold1,
                        typename Tail::type_t&& threshold2,
                        typename Tail::v2_select_t::type_t ATR,
                        typename Tail::v2_select_t::type_t ATInvR
                        );

                template<typename Result, typename Head1, typename Tail1, typename Head2, typename Tail2, typename ResEnc = typename TypeMap<Result>::v2_encoded_t, typename T1Enc = typename TypeMap<Tail1>::v2_encoded_t, typename T2Enc = typename TypeMap<Tail2>::v2_encoded_t>
                std::tuple<BAT<v2_void_t, Result>*, std::vector<bool>*, std::vector<bool>*>
                aggregate_mul_sumAN(
                        BAT<Head1, Tail1>* arg1,
                        BAT<Head2, Tail2>* arg2,
                        typename Result::type_t init = typename Result::type_t(0),
                        typename ResEnc::type_t RA = std::get<ANParametersSelector<ResEnc>::As->size() - 1>(*ANParametersSelector<ResEnc>::As),
                        typename ResEnc::type_t RAInv = std::get<ANParametersSelector<ResEnc>::Ainvs->size() - 1>(*ANParametersSelector<ResEnc>::Ainvs)
                        );

            }

#define V2_SELECT2_AN_SUB(SELECT, V2TYPE) \
extern template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> selectAN<std::greater, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t && th1, typename V2TYPE::type_t && th2); \
extern template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> selectAN<std::greater_equal, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg,typename V2TYPE::type_t && th1,typename V2TYPE::type_t && th2); \
extern template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> selectAN<std::equal_to, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg,typename V2TYPE::type_t && th1,typename V2TYPE::type_t && th2); \
extern template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> selectAN<std::less_equal, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg,typename V2TYPE::type_t && th1,typename V2TYPE::type_t && th2); \
extern template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> selectAN<std::less, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg,typename V2TYPE::type_t && th1,typename V2TYPE::type_t && th2); \
extern template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> selectAN<std::not_equal_to, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg,typename V2TYPE::type_t && th1,typename V2TYPE::type_t && th2);

#define V2_SELECT2_AN(V2TYPE) \
V2_SELECT2_AN_SUB(std::greater, V2TYPE) \
V2_SELECT2_AN_SUB(std::greater_equal, V2TYPE) \
V2_SELECT2_AN_SUB(std::equal_to, V2TYPE) \
V2_SELECT2_AN_SUB(std::less_equal, V2TYPE) \
V2_SELECT2_AN_SUB(std::less, V2TYPE) \
V2_SELECT2_AN_SUB(std::not_equal_to, V2TYPE)

#define V2_SELECT1_AN(V2HEAD, V2TAIL) \
extern template std::pair<BAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, AN_indicator_vector*> selectAN<std::greater, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1); \
extern template std::pair<BAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, AN_indicator_vector*> selectAN<std::greater_equal, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1); \
extern template std::pair<BAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, AN_indicator_vector*> selectAN<std::equal_to, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1); \
extern template std::pair<BAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, AN_indicator_vector*> selectAN<std::less_equal, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1); \
extern template std::pair<BAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, AN_indicator_vector*> selectAN<std::less, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1); \
extern template std::pair<BAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, AN_indicator_vector*> selectAN<std::not_equal_to, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1);

#define V2_SELECT_AN(V2TAIL) \
V2_SELECT1_AN(v2_void_t, V2TAIL) \
V2_SELECT1_AN(v2_oid_t, V2TAIL) \
V2_SELECT1_AN(v2_id_t, V2TAIL) \
V2_SELECT1_AN(v2_size_t, V2TAIL) \
V2_SELECT1_AN(v2_tinyint_t, V2TAIL) \
V2_SELECT1_AN(v2_shortint_t, V2TAIL) \
V2_SELECT1_AN(v2_int_t, V2TAIL) \
V2_SELECT1_AN(v2_bigint_t, V2TAIL) \
V2_SELECT1_AN(v2_str_t, V2TAIL) \
V2_SELECT1_AN(v2_resoid_t, V2TAIL) \
V2_SELECT1_AN(v2_restiny_t, V2TAIL) \
V2_SELECT1_AN(v2_resshort_t, V2TAIL) \
V2_SELECT1_AN(v2_resint_t, V2TAIL) \
V2_SELECT1_AN(v2_resbigint_t, V2TAIL) \
V2_SELECT1_AN(v2_resstr_t, V2TAIL) \
V2_SELECT2_AN(V2TAIL)

            namespace scalar {
                V2_SELECT_AN(v2_restiny_t)
                V2_SELECT_AN(v2_resshort_t)
                V2_SELECT_AN(v2_resint_t)
                V2_SELECT_AN(v2_resbigint_t)
                V2_SELECT_AN(v2_resstr_t)
            }

            namespace sse {
                V2_SELECT_AN(v2_restiny_t)
                V2_SELECT_AN(v2_resshort_t)
                V2_SELECT_AN(v2_resint_t)
                V2_SELECT_AN(v2_resbigint_t)
                V2_SELECT_AN(v2_resstr_t)
            }

#undef V2_SELECT_AN
#undef V2_SELECT1_AN
#undef V2_SELECT2_AN
#undef V2_SELECT2_AN_SUB

#define V2_HASHJOIN_SUB_AN(V2HEAD, V2TAIL) \
extern template std::tuple<TempBAT<typename V2HEAD::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_void_t> * arg1, BAT<v2_void_t, V2TAIL> * arg2, hash_side_t side); \
extern template std::tuple<TempBAT<typename V2HEAD::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_oid_t> * arg1, BAT<v2_oid_t, V2TAIL> * arg2, hash_side_t side); \
extern template std::tuple<TempBAT<typename V2HEAD::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_id_t> * arg1, BAT<v2_id_t, V2TAIL> * arg2, hash_side_t side); \
extern template std::tuple<TempBAT<typename V2HEAD::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_size_t> * arg1, BAT<v2_size_t, V2TAIL> * arg2, hash_side_t side); \
extern template std::tuple<TempBAT<typename V2HEAD::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_tinyint_t> * arg1, BAT<v2_tinyint_t, V2TAIL> * arg2, hash_side_t side); \
extern template std::tuple<TempBAT<typename V2HEAD::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_shortint_t> * arg1, BAT<v2_shortint_t, V2TAIL> * arg2, hash_side_t side); \
extern template std::tuple<TempBAT<typename V2HEAD::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_int_t> * arg1, BAT<v2_int_t, V2TAIL> * arg2, hash_side_t side); \
extern template std::tuple<TempBAT<typename V2HEAD::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_bigint_t> * arg1, BAT<v2_bigint_t, V2TAIL> * arg2, hash_side_t side); \
extern template std::tuple<TempBAT<typename V2HEAD::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_str_t> * arg1, BAT<v2_str_t, V2TAIL> * arg2, hash_side_t side); \
extern template std::tuple<TempBAT<typename V2HEAD::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_restiny_t> * arg1, BAT<v2_restiny_t, V2TAIL> * arg2, hash_side_t side); \
extern template std::tuple<TempBAT<typename V2HEAD::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_resoid_t> * arg1, BAT<v2_resoid_t, V2TAIL> * arg2, hash_side_t side); \
extern template std::tuple<TempBAT<typename V2HEAD::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_resshort_t> * arg1, BAT<v2_resshort_t, V2TAIL> * arg2, hash_side_t side); \
extern template std::tuple<TempBAT<typename V2HEAD::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_resint_t> * arg1, BAT<v2_resint_t, V2TAIL> * arg2, hash_side_t side); \
extern template std::tuple<TempBAT<typename V2HEAD::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_resbigint_t> * arg1, BAT<v2_resbigint_t, V2TAIL> * arg2, hash_side_t side); \
extern template std::tuple<TempBAT<typename V2HEAD::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_resstr_t> * arg1, BAT<v2_resstr_t, V2TAIL> * arg2, hash_side_t side); \
extern template std::tuple<TempBAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_void_t> * arg1, BAT<v2_void_t, V2TAIL> * arg2, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1reenc, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1InvReenc, hash_side_t side); \
extern template std::tuple<TempBAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_oid_t> * arg1, BAT<v2_oid_t, V2TAIL> * arg2, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1reenc, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1InvReenc, hash_side_t side); \
extern template std::tuple<TempBAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_id_t> * arg1, BAT<v2_id_t, V2TAIL> * arg2, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1reenc, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1InvReenc, hash_side_t side); \
extern template std::tuple<TempBAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_size_t> * arg1, BAT<v2_size_t, V2TAIL> * arg2, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1reenc, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1InvReenc, hash_side_t side); \
extern template std::tuple<TempBAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_tinyint_t> * arg1, BAT<v2_tinyint_t, V2TAIL> * arg2, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1reenc, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1InvReenc, hash_side_t side); \
extern template std::tuple<TempBAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_shortint_t> * arg1, BAT<v2_shortint_t, V2TAIL> * arg2, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1reenc, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1InvReenc, hash_side_t side); \
extern template std::tuple<TempBAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_int_t> * arg1, BAT<v2_int_t, V2TAIL> * arg2, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1reenc, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1InvReenc, hash_side_t side); \
extern template std::tuple<TempBAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_bigint_t> * arg1, BAT<v2_bigint_t, V2TAIL> * arg2, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1reenc, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1InvReenc, hash_side_t side); \
extern template std::tuple<TempBAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_str_t> * arg1, BAT<v2_str_t, V2TAIL> * arg2, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1reenc, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1InvReenc, hash_side_t side); \
extern template std::tuple<TempBAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_restiny_t> * arg1, BAT<v2_restiny_t, V2TAIL> * arg2, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1reenc, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1InvReenc, hash_side_t side); \
extern template std::tuple<TempBAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_resoid_t> * arg1, BAT<v2_resoid_t, V2TAIL> * arg2, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1reenc, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1InvReenc, hash_side_t side); \
extern template std::tuple<TempBAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_resshort_t> * arg1, BAT<v2_resshort_t, V2TAIL> * arg2, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1reenc, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1InvReenc, hash_side_t side); \
extern template std::tuple<TempBAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_resint_t> * arg1, BAT<v2_resint_t, V2TAIL> * arg2, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1reenc, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1InvReenc, hash_side_t side); \
extern template std::tuple<TempBAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_resbigint_t> * arg1, BAT<v2_resbigint_t, V2TAIL> * arg2, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1reenc, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1InvReenc, hash_side_t side); \
extern template std::tuple<TempBAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_resstr_t> * arg1, BAT<v2_resstr_t, V2TAIL> * arg2, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1reenc, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1InvReenc, hash_side_t side); \
extern template std::tuple<TempBAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename TypeMap<V2TAIL>::v2_encoded_t::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_void_t> * arg1, BAT<v2_void_t, V2TAIL> * arg2, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1reenc, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1InvReenc, typename TypeMap<V2TAIL>::v2_encoded_t::type_t AT2reenc, typename TypeMap<V2TAIL>::v2_encoded_t::type_t AT2InvReenc, hash_side_t side); \
extern template std::tuple<TempBAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename TypeMap<V2TAIL>::v2_encoded_t::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_oid_t> * arg1, BAT<v2_oid_t, V2TAIL> * arg2, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1reenc, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1InvReenc, typename TypeMap<V2TAIL>::v2_encoded_t::type_t AT2reenc, typename TypeMap<V2TAIL>::v2_encoded_t::type_t AT2InvReenc, hash_side_t side); \
extern template std::tuple<TempBAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename TypeMap<V2TAIL>::v2_encoded_t::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_id_t> * arg1, BAT<v2_id_t, V2TAIL> * arg2, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1reenc, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1InvReenc, typename TypeMap<V2TAIL>::v2_encoded_t::type_t AT2reenc, typename TypeMap<V2TAIL>::v2_encoded_t::type_t AT2InvReenc, hash_side_t side); \
extern template std::tuple<TempBAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename TypeMap<V2TAIL>::v2_encoded_t::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_size_t> * arg1, BAT<v2_size_t, V2TAIL> * arg2, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1reenc, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1InvReenc, typename TypeMap<V2TAIL>::v2_encoded_t::type_t AT2reenc, typename TypeMap<V2TAIL>::v2_encoded_t::type_t AT2InvReenc, hash_side_t side); \
extern template std::tuple<TempBAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename TypeMap<V2TAIL>::v2_encoded_t::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_tinyint_t> * arg1, BAT<v2_tinyint_t, V2TAIL> * arg2, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1reenc, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1InvReenc, typename TypeMap<V2TAIL>::v2_encoded_t::type_t AT2reenc, typename TypeMap<V2TAIL>::v2_encoded_t::type_t AT2InvReenc, hash_side_t side); \
extern template std::tuple<TempBAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename TypeMap<V2TAIL>::v2_encoded_t::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_shortint_t> * arg1, BAT<v2_shortint_t, V2TAIL> * arg2, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1reenc, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1InvReenc, typename TypeMap<V2TAIL>::v2_encoded_t::type_t AT2reenc, typename TypeMap<V2TAIL>::v2_encoded_t::type_t AT2InvReenc, hash_side_t side); \
extern template std::tuple<TempBAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename TypeMap<V2TAIL>::v2_encoded_t::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_int_t> * arg1, BAT<v2_int_t, V2TAIL> * arg2, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1reenc, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1InvReenc, typename TypeMap<V2TAIL>::v2_encoded_t::type_t AT2reenc, typename TypeMap<V2TAIL>::v2_encoded_t::type_t AT2InvReenc, hash_side_t side); \
extern template std::tuple<TempBAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename TypeMap<V2TAIL>::v2_encoded_t::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_bigint_t> * arg1, BAT<v2_bigint_t, V2TAIL> * arg2, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1reenc, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1InvReenc, typename TypeMap<V2TAIL>::v2_encoded_t::type_t AT2reenc, typename TypeMap<V2TAIL>::v2_encoded_t::type_t AT2InvReenc, hash_side_t side); \
extern template std::tuple<TempBAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename TypeMap<V2TAIL>::v2_encoded_t::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_str_t> * arg1, BAT<v2_str_t, V2TAIL> * arg2, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1reenc, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1InvReenc, typename TypeMap<V2TAIL>::v2_encoded_t::type_t AT2reenc, typename TypeMap<V2TAIL>::v2_encoded_t::type_t AT2InvReenc, hash_side_t side); \
extern template std::tuple<TempBAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename TypeMap<V2TAIL>::v2_encoded_t::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_restiny_t> * arg1, BAT<v2_restiny_t, V2TAIL> * arg2, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1reenc, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1InvReenc, typename TypeMap<V2TAIL>::v2_encoded_t::type_t AT2reenc, typename TypeMap<V2TAIL>::v2_encoded_t::type_t AT2InvReenc, hash_side_t side); \
extern template std::tuple<TempBAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename TypeMap<V2TAIL>::v2_encoded_t::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_resoid_t> * arg1, BAT<v2_resoid_t, V2TAIL> * arg2, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1reenc, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1InvReenc, typename TypeMap<V2TAIL>::v2_encoded_t::type_t AT2reenc, typename TypeMap<V2TAIL>::v2_encoded_t::type_t AT2InvReenc, hash_side_t side); \
extern template std::tuple<TempBAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename TypeMap<V2TAIL>::v2_encoded_t::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_resshort_t> * arg1, BAT<v2_resshort_t, V2TAIL> * arg2, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1reenc, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1InvReenc, typename TypeMap<V2TAIL>::v2_encoded_t::type_t AT2reenc, typename TypeMap<V2TAIL>::v2_encoded_t::type_t AT2InvReenc, hash_side_t side); \
extern template std::tuple<TempBAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename TypeMap<V2TAIL>::v2_encoded_t::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_resint_t> * arg1, BAT<v2_resint_t, V2TAIL> * arg2, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1reenc, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1InvReenc, typename TypeMap<V2TAIL>::v2_encoded_t::type_t AT2reenc, typename TypeMap<V2TAIL>::v2_encoded_t::type_t AT2InvReenc, hash_side_t side); \
extern template std::tuple<TempBAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename TypeMap<V2TAIL>::v2_encoded_t::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_resbigint_t> * arg1, BAT<v2_resbigint_t, V2TAIL> * arg2, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1reenc, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1InvReenc, typename TypeMap<V2TAIL>::v2_encoded_t::type_t AT2reenc, typename TypeMap<V2TAIL>::v2_encoded_t::type_t AT2InvReenc, hash_side_t side); \
extern template std::tuple<TempBAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename TypeMap<V2TAIL>::v2_encoded_t::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> hashjoinAN(BAT<V2HEAD, v2_resstr_t> * arg1, BAT<v2_resstr_t, V2TAIL> * arg2, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1reenc, typename TypeMap<V2HEAD>::v2_encoded_t::type_t AH1InvReenc, typename TypeMap<V2TAIL>::v2_encoded_t::type_t AT2reenc, typename TypeMap<V2TAIL>::v2_encoded_t::type_t AT2InvReenc, hash_side_t side);

#define V2_HASHJOIN_AN(V2TYPE) \
V2_HASHJOIN_SUB_AN(v2_void_t, V2TYPE) \
V2_HASHJOIN_SUB_AN(v2_oid_t, V2TYPE) \
V2_HASHJOIN_SUB_AN(v2_id_t, V2TYPE) \
V2_HASHJOIN_SUB_AN(v2_size_t, V2TYPE) \
V2_HASHJOIN_SUB_AN(v2_tinyint_t, V2TYPE) \
V2_HASHJOIN_SUB_AN(v2_shortint_t, V2TYPE) \
V2_HASHJOIN_SUB_AN(v2_int_t, V2TYPE) \
V2_HASHJOIN_SUB_AN(v2_bigint_t, V2TYPE) \
V2_HASHJOIN_SUB_AN(v2_str_t, V2TYPE) \
V2_HASHJOIN_SUB_AN(v2_restiny_t, V2TYPE) \
V2_HASHJOIN_SUB_AN(v2_resoid_t, V2TYPE) \
V2_HASHJOIN_SUB_AN(v2_resshort_t, V2TYPE) \
V2_HASHJOIN_SUB_AN(v2_resint_t, V2TYPE) \
V2_HASHJOIN_SUB_AN(v2_resbigint_t, V2TYPE) \
V2_HASHJOIN_SUB_AN(v2_resstr_t, V2TYPE)

            V2_HASHJOIN_AN(v2_void_t)
            V2_HASHJOIN_AN(v2_oid_t)
            V2_HASHJOIN_AN(v2_id_t)
            V2_HASHJOIN_AN(v2_size_t)
            V2_HASHJOIN_AN(v2_tinyint_t)
            V2_HASHJOIN_AN(v2_shortint_t)
            V2_HASHJOIN_AN(v2_int_t)
            V2_HASHJOIN_AN(v2_bigint_t)
            V2_HASHJOIN_AN(v2_str_t)
            V2_HASHJOIN_AN(v2_resoid_t)
            V2_HASHJOIN_AN(v2_restiny_t)
            V2_HASHJOIN_AN(v2_resshort_t)
            V2_HASHJOIN_AN(v2_resint_t)
            V2_HASHJOIN_AN(v2_resbigint_t)
            V2_HASHJOIN_AN(v2_resstr_t)

#undef V2_HASHJOIN
#undef V2_HASHJOIN_SUB

#define V2_MATCHJOIN_AN_SUB4(H1, T1, H2, T2) \
extern template std::tuple<TempBAT<typename H1::v2_select_t, typename T2::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> matchjoinAN(BAT<H1, T1> *arg1, BAT<H2, T2> *arg2); \
extern template std::tuple<TempBAT<typename H1::v2_select_t, typename T2::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> matchjoinAN(BAT<H1, T1> *arg1, BAT<H2, T2> *arg2, typename TypeMap<H1>::v2_encoded_t::type_t AH1reenc, typename TypeMap<H1>::v2_encoded_t::type_t AH1InvReenc); \
extern template std::tuple<TempBAT<typename H1::v2_select_t, typename T2::v2_select_t>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*, std::vector<bool>*> matchjoinAN(BAT<H1, T1> *arg1, BAT<H2, T2> *arg2, typename TypeMap<H1>::v2_encoded_t::type_t AH1reenc, typename TypeMap<H1>::v2_encoded_t::type_t AH1InvReenc, typename TypeMap<T1>::v2_encoded_t::type_t AT2Reenc, typename TypeMap<T1>::v2_encoded_t::type_t AT2InvReenc);

#define V2_MATCHJOIN_AN_SUB2(H1, T1) \
V2_MATCHJOIN_AN_SUB4(H1, T1, T1, v2_void_t) \
V2_MATCHJOIN_AN_SUB4(H1, T1, T1, v2_oid_t) \
V2_MATCHJOIN_AN_SUB4(H1, T1, T1, v2_tinyint_t) \
V2_MATCHJOIN_AN_SUB4(H1, T1, T1, v2_shortint_t) \
V2_MATCHJOIN_AN_SUB4(H1, T1, T1, v2_int_t) \
V2_MATCHJOIN_AN_SUB4(H1, T1, T1, v2_bigint_t) \
V2_MATCHJOIN_AN_SUB4(H1, T1, T1, v2_resoid_t) \
V2_MATCHJOIN_AN_SUB4(H1, T1, T1, v2_restinyint_t) \
V2_MATCHJOIN_AN_SUB4(H1, T1, T1, v2_resshortint_t) \
V2_MATCHJOIN_AN_SUB4(H1, T1, T1, v2_resint_t) \
V2_MATCHJOIN_AN_SUB4(H1, T1, T1, v2_resbigint_t)

#define V2_MATCHJOIN_AN_SUB1(H1) \
V2_MATCHJOIN_AN_SUB2(H1, v2_void_t) \
V2_MATCHJOIN_AN_SUB2(H1, v2_oid_t) \
V2_MATCHJOIN_AN_SUB2(H1, v2_tinyint_t) \
V2_MATCHJOIN_AN_SUB2(H1, v2_shortint_t) \
V2_MATCHJOIN_AN_SUB2(H1, v2_int_t) \
V2_MATCHJOIN_AN_SUB2(H1, v2_bigint_t) \
V2_MATCHJOIN_AN_SUB2(H1, v2_resoid_t) \
V2_MATCHJOIN_AN_SUB2(H1, v2_restinyint_t) \
V2_MATCHJOIN_AN_SUB2(H1, v2_resshortint_t) \
V2_MATCHJOIN_AN_SUB2(H1, v2_resint_t) \
V2_MATCHJOIN_AN_SUB2(H1, v2_resbigint_t)

            V2_MATCHJOIN_AN_SUB1(v2_void_t)
            V2_MATCHJOIN_AN_SUB1(v2_oid_t)
            V2_MATCHJOIN_AN_SUB1(v2_tinyint_t)
            V2_MATCHJOIN_AN_SUB1(v2_shortint_t)
            V2_MATCHJOIN_AN_SUB1(v2_int_t)
            V2_MATCHJOIN_AN_SUB1(v2_bigint_t)
            V2_MATCHJOIN_AN_SUB1(v2_resoid_t)
            V2_MATCHJOIN_AN_SUB1(v2_restinyint_t)
            V2_MATCHJOIN_AN_SUB1(v2_resshortint_t)
            V2_MATCHJOIN_AN_SUB1(v2_resint_t)
            V2_MATCHJOIN_AN_SUB1(v2_resbigint_t)

#undef V2_MATCHJOIN_AN_SUB1
#undef V2_MATCHJOIN_AN_SUB2
#undef V2_MATCHJOIN_AN_SUB4

#define V2_AGGREGATE_MUL_SUM_AN(Result, ResEnc, Head1, Tail1, Head2, Tail2) \
extern template std::tuple<BAT<v2_void_t, Result>*, std::vector<bool>*, std::vector<bool>*> aggregate_mul_sumAN(BAT<Head1, Tail1>* arg1, BAT<Head2, Tail2>* arg2, typename Result::type_t init, typename ResEnc::type_t RA, typename ResEnc::type_t RAInv);

#define V2_AGGREGATE_MUL_SUM_AN_SUB(Result, Tail1, Tail2) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_void_t, CONCAT(v2_, Tail1, _t), v2_void_t, CONCAT(v2_, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_void_t, CONCAT(v2_, Tail1, _t), v2_oid_t, CONCAT(v2_, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_oid_t, CONCAT(v2_, Tail1, _t), v2_void_t, CONCAT(v2_, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_oid_t, CONCAT(v2_, Tail1, _t), v2_oid_t, CONCAT(v2_, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_void_t, CONCAT(v2_res, Tail1, _t), v2_void_t, CONCAT(v2_, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_void_t, CONCAT(v2_res, Tail1, _t), v2_oid_t, CONCAT(v2_, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_oid_t, CONCAT(v2_res, Tail1, _t), v2_void_t, CONCAT(v2_, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_oid_t, CONCAT(v2_res, Tail1, _t), v2_oid_t, CONCAT(v2_, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_void_t, CONCAT(v2_, Tail1, _t), v2_void_t, CONCAT(v2_res, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_void_t, CONCAT(v2_, Tail1, _t), v2_oid_t, CONCAT(v2_res, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_oid_t, CONCAT(v2_, Tail1, _t), v2_void_t, CONCAT(v2_res, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_oid_t, CONCAT(v2_, Tail1, _t), v2_oid_t, CONCAT(v2_res, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_void_t, CONCAT(v2_res, Tail1, _t), v2_void_t, CONCAT(v2_res, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_void_t, CONCAT(v2_res, Tail1, _t), v2_oid_t, CONCAT(v2_res, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_oid_t, CONCAT(v2_res, Tail1, _t), v2_void_t, CONCAT(v2_res, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_oid_t, CONCAT(v2_res, Tail1, _t), v2_oid_t, CONCAT(v2_res, Tail2, _t)) \
\
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_void_t, CONCAT(v2_, Tail1, _t), v2_resoid_t, CONCAT(v2_, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_oid_t, CONCAT(v2_, Tail1, _t), v2_resoid_t, CONCAT(v2_, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_resoid_t, CONCAT(v2_, Tail1, _t), v2_resoid_t, CONCAT(v2_, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_resoid_t, CONCAT(v2_, Tail1, _t), v2_void_t, CONCAT(v2_, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_resoid_t, CONCAT(v2_, Tail1, _t), v2_oid_t, CONCAT(v2_, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_resoid_t, CONCAT(v2_, Tail1, _t), v2_resoid_t, CONCAT(v2_, Tail2, _t)) \
\
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_void_t, CONCAT(v2_res, Tail1, _t), v2_resoid_t, CONCAT(v2_, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_oid_t, CONCAT(v2_res, Tail1, _t), v2_resoid_t, CONCAT(v2_, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_resoid_t, CONCAT(v2_res, Tail1, _t), v2_resoid_t, CONCAT(v2_, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_resoid_t, CONCAT(v2_res, Tail1, _t), v2_void_t, CONCAT(v2_, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_resoid_t, CONCAT(v2_res, Tail1, _t), v2_oid_t, CONCAT(v2_, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_resoid_t, CONCAT(v2_res, Tail1, _t), v2_resoid_t, CONCAT(v2_, Tail2, _t)) \
\
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_void_t, CONCAT(v2_, Tail1, _t), v2_resoid_t, CONCAT(v2_res, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_oid_t, CONCAT(v2_, Tail1, _t), v2_resoid_t, CONCAT(v2_res, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_resoid_t, CONCAT(v2_, Tail1, _t), v2_resoid_t, CONCAT(v2_res, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_resoid_t, CONCAT(v2_, Tail1, _t), v2_void_t, CONCAT(v2_res, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_resoid_t, CONCAT(v2_, Tail1, _t), v2_oid_t, CONCAT(v2_res, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_resoid_t, CONCAT(v2_, Tail1, _t), v2_resoid_t, CONCAT(v2_res, Tail2, _t)) \
\
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_void_t, CONCAT(v2_res, Tail1, _t), v2_resoid_t, CONCAT(v2_res, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_oid_t, CONCAT(v2_res, Tail1, _t), v2_resoid_t, CONCAT(v2_res, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_resoid_t, CONCAT(v2_res, Tail1, _t), v2_resoid_t, CONCAT(v2_res, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_resoid_t, CONCAT(v2_res, Tail1, _t), v2_void_t, CONCAT(v2_res, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_resoid_t, CONCAT(v2_res, Tail1, _t), v2_oid_t, CONCAT(v2_res, Tail2, _t)) \
V2_AGGREGATE_MUL_SUM_AN(CONCAT(v2_, Result, _t), CONCAT(v2_, Result, _t), v2_resoid_t, CONCAT(v2_res, Tail1, _t), v2_resoid_t, CONCAT(v2_res, Tail2, _t))

#define AHEAD_AGGREGATE_MUL_SUM_AN \
V2_AGGREGATE_MUL_SUM_AN_SUB(resshort, tinyint, tinyint) \
V2_AGGREGATE_MUL_SUM_AN_SUB(resint, tinyint, tinyint) \
V2_AGGREGATE_MUL_SUM_AN_SUB(resbigint, tinyint, tinyint) \
V2_AGGREGATE_MUL_SUM_AN_SUB(resint, tinyint, shortint) \
V2_AGGREGATE_MUL_SUM_AN_SUB(resint, shortint, tinyint) \
V2_AGGREGATE_MUL_SUM_AN_SUB(resbigint, tinyint, shortint) \
V2_AGGREGATE_MUL_SUM_AN_SUB(resbigint, shortint, tinyint) \
V2_AGGREGATE_MUL_SUM_AN_SUB(resbigint, tinyint, int) \
V2_AGGREGATE_MUL_SUM_AN_SUB(resbigint, int, tinyint) \
V2_AGGREGATE_MUL_SUM_AN_SUB(resbigint, shortint, int) \
V2_AGGREGATE_MUL_SUM_AN_SUB(resbigint, int, shortint) \
V2_AGGREGATE_MUL_SUM_AN_SUB(resbigint, int, int) \
V2_AGGREGATE_MUL_SUM_AN_SUB(resbigint, bigint, int) \
V2_AGGREGATE_MUL_SUM_AN_SUB(resbigint, int, bigint)

            namespace scalar {
                AHEAD_AGGREGATE_MUL_SUM_AN
            }

            namespace sse {
                AHEAD_AGGREGATE_MUL_SUM_AN
            }

#undef V2_AGGREGATE_MUL_SUM_AN_SUB
#undef V2_AGGREGATE_MUL_SUM_AN
#undef AHEAD_AGGREGATE_MUL_SUM_AN

        }
    }
}

#endif /* OPERATORSAN_HPP */
