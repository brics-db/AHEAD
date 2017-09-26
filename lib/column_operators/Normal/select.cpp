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
 * File:   select.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 10-04-2017 22:03
 */

#include "select.tcc"
#include <column_operators/functors.hpp>

namespace ahead {
    namespace bat {
        namespace ops {

#define V2_SELECT2_SUB2(SELECT1, SELECT2, V2TYPE) \
template BAT<v2_oid_t, typename V2TYPE::v2_select_t>* scalar::select<SELECT1, SELECT2, ahead::and_is, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t th1, typename V2TYPE::type_t th2); \
template BAT<v2_oid_t, typename V2TYPE::v2_select_t>* scalar::select<SELECT1, SELECT2, ahead::or_is, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t th1, typename V2TYPE::type_t th2); \
template BAT<v2_oid_t, typename V2TYPE::v2_select_t>* simd::sse::select<SELECT1, SELECT2, ahead::and_is, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t th1, typename V2TYPE::type_t th2); \
template BAT<v2_oid_t, typename V2TYPE::v2_select_t>* simd::sse::select<SELECT1, SELECT2, ahead::or_is, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t th1, typename V2TYPE::type_t th2);

#define V2_SELECT2_SUB(SELECT, V2TYPE) \
V2_SELECT2_SUB2(std::greater, SELECT, V2TYPE) \
V2_SELECT2_SUB2(std::greater_equal, SELECT, V2TYPE) \
V2_SELECT2_SUB2(std::equal_to, SELECT, V2TYPE) \
V2_SELECT2_SUB2(std::less_equal, SELECT, V2TYPE) \
V2_SELECT2_SUB2(std::less, SELECT, V2TYPE) \
V2_SELECT2_SUB2(std::not_equal_to, SELECT, V2TYPE)

#define V2_SELECT2(V2TYPE) \
V2_SELECT2_SUB(std::greater, V2TYPE) \
V2_SELECT2_SUB(std::greater_equal, V2TYPE) \
V2_SELECT2_SUB(std::equal_to, V2TYPE) \
V2_SELECT2_SUB(std::less_equal, V2TYPE) \
V2_SELECT2_SUB(std::less, V2TYPE) \
V2_SELECT2_SUB(std::not_equal_to, V2TYPE)

#define V2_SELECT(V2TYPE) \
template BAT<v2_oid_t, typename V2TYPE::v2_select_t>* scalar::select<std::greater, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t th1); \
template BAT<v2_oid_t, typename V2TYPE::v2_select_t>* scalar::select<std::greater_equal, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t th1); \
template BAT<v2_oid_t, typename V2TYPE::v2_select_t>* scalar::select<std::equal_to, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t th1); \
template BAT<v2_oid_t, typename V2TYPE::v2_select_t>* scalar::select<std::less_equal, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t th1); \
template BAT<v2_oid_t, typename V2TYPE::v2_select_t>* scalar::select<std::less, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t th1); \
template BAT<v2_oid_t, typename V2TYPE::v2_select_t>* simd::sse::select<std::greater, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t th1); \
template BAT<v2_oid_t, typename V2TYPE::v2_select_t>* simd::sse::select<std::greater_equal, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t th1); \
template BAT<v2_oid_t, typename V2TYPE::v2_select_t>* simd::sse::select<std::equal_to, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t th1); \
template BAT<v2_oid_t, typename V2TYPE::v2_select_t>* simd::sse::select<std::less_equal, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t th1); \
template BAT<v2_oid_t, typename V2TYPE::v2_select_t>* simd::sse::select<std::less, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t th1); \
V2_SELECT2(V2TYPE)

#define V2_SELECT_STR(V2TYPE) \
template BAT<v2_oid_t, typename V2TYPE::v2_select_t>* scalar::select<std::equal_to, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t th1); \
template BAT<v2_oid_t, typename V2TYPE::v2_select_t>* simd::sse::select<std::equal_to, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t th1); \
V2_SELECT2(V2TYPE);

            V2_SELECT(v2_id_t)
            V2_SELECT(v2_size_t)
            V2_SELECT(v2_tinyint_t)
            V2_SELECT(v2_shortint_t)
            V2_SELECT(v2_int_t)
            V2_SELECT(v2_bigint_t)
            V2_SELECT(v2_str_t)
            V2_SELECT(v2_resoid_t)
            V2_SELECT(v2_restiny_t)
            V2_SELECT(v2_resshort_t)
            V2_SELECT(v2_resint_t)
            V2_SELECT(v2_resbigint_t)

#undef V2_SELECT_STR
#undef V2_SELECT
#undef V2_SELECT2
#undef V2_SELECT2_SUB
#undef V2_SELECT2_SUB2

        }
    }
}
