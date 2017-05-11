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

#ifndef OPERATORSAN_TCC
#define OPERATORSAN_TCC

#include <column_storage/Storage.hpp>
#include <column_operators/AN/ANbase.hpp>
#include <column_operators/AN/selectAN.tcc>
#include <column_operators/AN/hashjoinAN.tcc>
#include <column_operators/AN/matchjoinAN.tcc>
#include <column_operators/AN/aggregateAN.tcc>
#include <column_operators/AN/encdecAN.tcc>
#include <column_operators/AN/groupbyAN.tcc>

namespace ahead {
    namespace bat {
        namespace ops {

#define V2_SELECT2_AN_SUB(SELECT, V2TYPE) \
extern template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> scalar::selectAN<std::greater, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t && th1, typename V2TYPE::type_t && th2); \
extern template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> scalar::selectAN<std::greater_equal, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg,typename V2TYPE::type_t && th1,typename V2TYPE::type_t && th2); \
extern template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> scalar::selectAN<std::equal_to, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg,typename V2TYPE::type_t && th1,typename V2TYPE::type_t && th2); \
extern template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> scalar::selectAN<std::less_equal, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg,typename V2TYPE::type_t && th1,typename V2TYPE::type_t && th2); \
extern template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> scalar::selectAN<std::less, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg,typename V2TYPE::type_t && th1,typename V2TYPE::type_t && th2); \
extern template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> scalar::selectAN<std::not_equal_to, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg,typename V2TYPE::type_t && th1,typename V2TYPE::type_t && th2); \
extern template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> sse::selectAN<std::greater, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t && th1, typename V2TYPE::type_t && th2); \
extern template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> sse::selectAN<std::greater_equal, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg,typename V2TYPE::type_t && th1,typename V2TYPE::type_t && th2); \
extern template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> sse::selectAN<std::equal_to, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg,typename V2TYPE::type_t && th1,typename V2TYPE::type_t && th2); \
extern template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> sse::selectAN<std::less_equal, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg,typename V2TYPE::type_t && th1,typename V2TYPE::type_t && th2); \
extern template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> sse::selectAN<std::less, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg,typename V2TYPE::type_t && th1,typename V2TYPE::type_t && th2); \
extern template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> sse::selectAN<std::not_equal_to, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg,typename V2TYPE::type_t && th1,typename V2TYPE::type_t && th2);

#define V2_SELECT2_AN(V2TYPE) \
V2_SELECT2_AN_SUB(std::greater, V2TYPE) \
V2_SELECT2_AN_SUB(std::greater_equal, V2TYPE) \
V2_SELECT2_AN_SUB(std::equal_to, V2TYPE) \
V2_SELECT2_AN_SUB(std::less_equal, V2TYPE) \
V2_SELECT2_AN_SUB(std::less, V2TYPE) \
V2_SELECT2_AN_SUB(std::not_equal_to, V2TYPE)

#define V2_SELECT1_AN(V2HEAD, V2TAIL) \
extern template std::pair<BAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, AN_indicator_vector*> scalar::selectAN<std::greater, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1); \
extern template std::pair<BAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, AN_indicator_vector*> scalar::selectAN<std::greater_equal, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1); \
extern template std::pair<BAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, AN_indicator_vector*> scalar::selectAN<std::equal_to, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1); \
extern template std::pair<BAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, AN_indicator_vector*> scalar::selectAN<std::less_equal, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1); \
extern template std::pair<BAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, AN_indicator_vector*> scalar::selectAN<std::less, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1); \
extern template std::pair<BAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, AN_indicator_vector*> scalar::selectAN<std::not_equal_to, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1); \
extern template std::pair<BAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, AN_indicator_vector*> sse::selectAN<std::greater, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1); \
extern template std::pair<BAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, AN_indicator_vector*> sse::selectAN<std::greater_equal, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1); \
extern template std::pair<BAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, AN_indicator_vector*> sse::selectAN<std::equal_to, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1); \
extern template std::pair<BAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, AN_indicator_vector*> sse::selectAN<std::less_equal, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1); \
extern template std::pair<BAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, AN_indicator_vector*> sse::selectAN<std::less, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1); \
extern template std::pair<BAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, AN_indicator_vector*> sse::selectAN<std::not_equal_to, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1);

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

            V2_SELECT_AN(v2_restiny_t)
            V2_SELECT_AN(v2_resshort_t)
            V2_SELECT_AN(v2_resint_t)
            V2_SELECT_AN(v2_resbigint_t)
            V2_SELECT_AN(v2_resstr_t)

#undef V2_SELECT_AN
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
extern template std::tuple<BAT<v2_void_t, Result>*, std::vector<bool>*, std::vector<bool>*> scalar::aggregate_mul_sumAN(BAT<Head1, Tail1>* arg1, BAT<Head2, Tail2>* arg2, typename Result::type_t init, typename ResEnc::type_t RA, typename ResEnc::type_t RAInv); \
extern template std::tuple<BAT<v2_void_t, Result>*, std::vector<bool>*, std::vector<bool>*> sse::aggregate_mul_sumAN(BAT<Head1, Tail1>* arg1, BAT<Head2, Tail2>* arg2, typename Result::type_t init, typename ResEnc::type_t RA, typename ResEnc::type_t RAInv);

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

            V2_AGGREGATE_MUL_SUM_AN_SUB(resshort, tinyint, tinyint)
            V2_AGGREGATE_MUL_SUM_AN_SUB(resint, tinyint, tinyint)
            V2_AGGREGATE_MUL_SUM_AN_SUB(resbigint, tinyint, tinyint)
            V2_AGGREGATE_MUL_SUM_AN_SUB(resint, tinyint, shortint)
            V2_AGGREGATE_MUL_SUM_AN_SUB(resint, shortint, tinyint)
            V2_AGGREGATE_MUL_SUM_AN_SUB(resbigint, tinyint, shortint)
            V2_AGGREGATE_MUL_SUM_AN_SUB(resbigint, shortint, tinyint)
            V2_AGGREGATE_MUL_SUM_AN_SUB(resbigint, tinyint, int)
            V2_AGGREGATE_MUL_SUM_AN_SUB(resbigint, int, tinyint)
            V2_AGGREGATE_MUL_SUM_AN_SUB(resbigint, shortint, int)
            V2_AGGREGATE_MUL_SUM_AN_SUB(resbigint, int, shortint)
            V2_AGGREGATE_MUL_SUM_AN_SUB(resbigint, int, int)
            V2_AGGREGATE_MUL_SUM_AN_SUB(resbigint, bigint, int)
            V2_AGGREGATE_MUL_SUM_AN_SUB(resbigint, int, bigint)

#undef V2_AGGREGATE_MUL_SUM_AN_SUB
#undef V2_AGGREGATE_MUL_SUM_AN

        }
    }
}

#endif /* OPERATORSAN_TCC */
