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
 * File:   aggregate.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 10-04-2017 22:02
 */

#include <column_operators/Normal/aggregate.tcc>

namespace ahead {
    namespace bat {
        namespace ops {

#define V2_AGGREGATE_SUM(V2RESULT, V2TYPE) \
template typename V2RESULT::type_t scalar::aggregate_sum<V2RESULT, v2_oid_t, V2TYPE>(BAT<v2_oid_t, V2TYPE> * arg); \
template typename V2RESULT::type_t sse::aggregate_sum<V2RESULT, v2_oid_t, V2TYPE>(BAT<v2_oid_t, V2TYPE> * arg);

            V2_AGGREGATE_SUM(v2_tinyint_t, v2_tinyint_t)
            V2_AGGREGATE_SUM(v2_shortint_t, v2_tinyint_t)
            V2_AGGREGATE_SUM(v2_int_t, v2_tinyint_t)
            V2_AGGREGATE_SUM(v2_bigint_t, v2_tinyint_t)
            V2_AGGREGATE_SUM(v2_shortint_t, v2_shortint_t)
            V2_AGGREGATE_SUM(v2_int_t, v2_shortint_t)
            V2_AGGREGATE_SUM(v2_bigint_t, v2_shortint_t)
            V2_AGGREGATE_SUM(v2_int_t, v2_int_t)
            V2_AGGREGATE_SUM(v2_bigint_t, v2_int_t)
            V2_AGGREGATE_SUM(v2_bigint_t, v2_bigint_t)

#undef V2_AGGREGATE_SUM

#define V2_AGGREGATE_MUL_SUM(V2RESULT, V2TAIL1, V2TAIL2) \
template BAT<v2_void_t, V2RESULT> * scalar::aggregate_mul_sum<V2RESULT, v2_oid_t, V2TAIL1, v2_oid_t, V2TAIL2>(BAT<v2_oid_t, V2TAIL1> * arg1, BAT<v2_oid_t, V2TAIL2> * arg2, typename V2RESULT::type_t init); \
template BAT<v2_void_t, V2RESULT> * sse::aggregate_mul_sum<V2RESULT, v2_oid_t, V2TAIL1, v2_oid_t, V2TAIL2>(BAT<v2_oid_t, V2TAIL1> * arg1, BAT<v2_oid_t, V2TAIL2> * arg2, typename V2RESULT::type_t init);

            // V2_AGGREGATE_MUL_SUM(v2_shortint_t, v2_tinyint_t, v2_tinyint_t)
            // V2_AGGREGATE_MUL_SUM(v2_int_t, v2_tinyint_t, v2_tinyint_t)
            // V2_AGGREGATE_MUL_SUM(v2_bigint_t, v2_tinyint_t, v2_tinyint_t)
            // V2_AGGREGATE_MUL_SUM(v2_int_t, v2_tinyint_t, v2_shortint_t)
            // V2_AGGREGATE_MUL_SUM(v2_int_t, v2_shortint_t, v2_tinyint_t)
            // V2_AGGREGATE_MUL_SUM(v2_bigint_t, v2_tinyint_t, v2_shortint_t)
            // V2_AGGREGATE_MUL_SUM(v2_bigint_t, v2_shortint_t, v2_tinyint_t)
            // V2_AGGREGATE_MUL_SUM(v2_bigint_t, v2_tinyint_t, v2_int_t)
            V2_AGGREGATE_MUL_SUM(v2_bigint_t, v2_int_t, v2_tinyint_t)
            // V2_AGGREGATE_MUL_SUM(v2_bigint_t, v2_shortint_t, v2_int_t)
            // V2_AGGREGATE_MUL_SUM(v2_bigint_t, v2_int_t, v2_shortint_t)
            // V2_AGGREGATE_MUL_SUM(v2_bigint_t, v2_int_t, v2_int_t)
            // V2_AGGREGATE_MUL_SUM(v2_bigint_t, v2_bigint_t, v2_int_t)
            // V2_AGGREGATE_MUL_SUM(v2_bigint_t, v2_int_t, v2_bigint_t)

#undef V2_AGGREGATE_MUL_SUM

        }
    }
}
