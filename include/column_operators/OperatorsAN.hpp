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
extern template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, std::vector<bool>*> selectAN<std::greater, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t && th1, typename V2TYPE::type_t && th2); \
extern template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, std::vector<bool>*> selectAN<std::greater_equal, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg,typename V2TYPE::type_t && th1,typename V2TYPE::type_t && th2); \
extern template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, std::vector<bool>*> selectAN<std::equal_to, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg,typename V2TYPE::type_t && th1,typename V2TYPE::type_t && th2); \
extern template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, std::vector<bool>*> selectAN<std::less_equal, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg,typename V2TYPE::type_t && th1,typename V2TYPE::type_t && th2); \
extern template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, std::vector<bool>*> selectAN<std::less, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg,typename V2TYPE::type_t && th1,typename V2TYPE::type_t && th2); \
extern template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, std::vector<bool>*> selectAN<std::not_equal_to, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg,typename V2TYPE::type_t && th1,typename V2TYPE::type_t && th2);

#define V2_SELECT2_AN(V2TYPE) \
V2_SELECT2_AN_SUB(std::greater, V2TYPE) \
V2_SELECT2_AN_SUB(std::greater_equal, V2TYPE) \
V2_SELECT2_AN_SUB(std::equal_to, V2TYPE) \
V2_SELECT2_AN_SUB(std::less_equal, V2TYPE) \
V2_SELECT2_AN_SUB(std::less, V2TYPE) \
V2_SELECT2_AN_SUB(std::not_equal_to, V2TYPE)

#define V2_SELECT1_AN(V2HEAD, V2TAIL) \
extern template std::pair<BAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*> selectAN<std::greater, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1); \
extern template std::pair<BAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*> selectAN<std::greater_equal, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1); \
extern template std::pair<BAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*> selectAN<std::equal_to, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1); \
extern template std::pair<BAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*> selectAN<std::less_equal, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1); \
extern template std::pair<BAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*> selectAN<std::less, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1); \
extern template std::pair<BAT<typename TypeMap<V2HEAD>::v2_encoded_t::v2_select_t, typename V2TAIL::v2_select_t>*, std::vector<bool>*> selectAN<std::not_equal_to, V2HEAD, V2TAIL> (BAT<V2HEAD, V2TAIL>* arg, typename V2TAIL::type_t && th1); \

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

        }
    }
}

#endif /* OPERATORSAN_TCC */
