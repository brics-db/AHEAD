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
 * File:   aggregateAN.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 10. April 2017, 22:11
 */

#include <column_operators/ANbase.hpp>
#include "aggregateAN.tcc"

namespace ahead {
    namespace bat {
        namespace ops {

#define V2_AGGREGATE_MUL_SUM_AN(Result, ResEnc, Head1, Tail1, Head2, Tail2) \
template std::tuple<BAT<v2_void_t, Result>*, std::vector<bool>*, std::vector<bool>*> scalar::aggregate_mul_sumAN(BAT<Head1, Tail1>* arg1, BAT<Head2, Tail2>* arg2, typename Result::type_t init, typename ResEnc::type_t RA, typename ResEnc::type_t RAInv); \
template std::tuple<BAT<v2_void_t, Result>*, std::vector<bool>*, std::vector<bool>*> sse::aggregate_mul_sumAN(BAT<Head1, Tail1>* arg1, BAT<Head2, Tail2>* arg2, typename Result::type_t init, typename ResEnc::type_t RA, typename ResEnc::type_t RAInv);

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

            // V2_AGGREGATE_MUL_SUM_AN_SUB(resshort, tinyint, tinyint)
            // V2_AGGREGATE_MUL_SUM_AN_SUB(resint, tinyint, tinyint)
            // V2_AGGREGATE_MUL_SUM_AN_SUB(resbigint, tinyint, tinyint)
            // V2_AGGREGATE_MUL_SUM_AN_SUB(resint, tinyint, shortint)
            // V2_AGGREGATE_MUL_SUM_AN_SUB(resint, shortint, tinyint)
            // V2_AGGREGATE_MUL_SUM_AN_SUB(resbigint, tinyint, shortint)
            // V2_AGGREGATE_MUL_SUM_AN_SUB(resbigint, shortint, tinyint)
            // V2_AGGREGATE_MUL_SUM_AN_SUB(resbigint, tinyint, int)
            // V2_AGGREGATE_MUL_SUM_AN_SUB(resbigint, int, tinyint)
            // V2_AGGREGATE_MUL_SUM_AN_SUB(resbigint, shortint, int)
            // V2_AGGREGATE_MUL_SUM_AN_SUB(resbigint, int, shortint)
            // V2_AGGREGATE_MUL_SUM_AN_SUB(resbigint, int, int)
            // V2_AGGREGATE_MUL_SUM_AN_SUB(resbigint, bigint, int)
            // V2_AGGREGATE_MUL_SUM_AN_SUB(resbigint, int, bigint)

            V2_AGGREGATE_MUL_SUM_AN(v2_resbigint_t, v2_resbigint_t, v2_resoid_t, v2_resint_t, v2_resoid_t, v2_restiny_t)

#define V2_AGGREGATE_SUM_GROUPED(Result, Head, Tail)                                                                                                 \
template std::tuple<BAT<v2_void_t, Result> *, AN_indicator_vector *, AN_indicator_vector *, AN_indicator_vector *>                                   \
    aggregate_sum_groupedAN(                                                                                                                         \
            BAT<Head, Tail> * bat,                                                                                                                   \
            BAT<v2_void_t, v2_resoid_t> * grouping,                                                                                                  \
            size_t numGroups,                                                                                                                        \
			typename Result::type_t AResult,                                                                                                         \
			typename Result::type_t AResultInv,                                                                                                      \
            resoid_t AOID);

            V2_AGGREGATE_SUM_GROUPED(v2_resbigint_t, v2_oid_t, v2_int_t)
            V2_AGGREGATE_SUM_GROUPED(v2_resbigint_t, v2_void_t, v2_int_t)
            V2_AGGREGATE_SUM_GROUPED(v2_resbigint_t, v2_resoid_t, v2_int_t)
            V2_AGGREGATE_SUM_GROUPED(v2_resbigint_t, v2_oid_t, v2_resint_t)
            V2_AGGREGATE_SUM_GROUPED(v2_resbigint_t, v2_void_t, v2_resint_t)
            V2_AGGREGATE_SUM_GROUPED(v2_resbigint_t, v2_resoid_t, v2_resint_t)

        }
    }
}
