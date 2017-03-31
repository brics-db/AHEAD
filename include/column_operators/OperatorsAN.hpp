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
#include <column_operators/Normal/miscellaneous.tcc>
#include <column_operators/AN/selectAN.tcc>
#include <column_operators/AN/hashjoinAN.tcc>
#include <column_operators/AN/matchjoinAN.tcc>
#include <column_operators/AN/aggregateAN.tcc>
#include <column_operators/AN/encdecAN.tcc>
#include <column_operators/AN/groupbyAN.tcc>

namespace v2 {
    namespace bat {
        namespace ops {

            extern template TempBAT<v2_void_t, v2_restiny_t>* copy(BAT<v2_void_t, v2_restiny_t>* arg);
            extern template TempBAT<v2_void_t, v2_resshort_t>* copy(BAT<v2_void_t, v2_resshort_t>* arg);
            extern template TempBAT<v2_void_t, v2_resint_t>* copy(BAT<v2_void_t, v2_resint_t>* arg);
            extern template TempBAT<v2_void_t, v2_resbigint_t>* copy(BAT<v2_void_t, v2_resbigint_t>* arg);
            extern template TempBAT<v2_void_t, v2_resstr_t>* copy(BAT<v2_void_t, v2_resstr_t>* arg);

#define V2_SELECT(V2TYPE, V2SELECTTYPE, TYPE) \
extern template std::pair<BAT<v2_resoid_t, V2SELECTTYPE>*, std::vector<bool>*> selectAN<std::greater, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1); \
extern template std::pair<BAT<v2_resoid_t, V2SELECTTYPE>*, std::vector<bool>*> selectAN<std::greater_equal, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1); \
extern template std::pair<BAT<v2_resoid_t, V2SELECTTYPE>*, std::vector<bool>*> selectAN<std::equal_to, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1); \
extern template std::pair<BAT<v2_resoid_t, V2SELECTTYPE>*, std::vector<bool>*> selectAN<std::less_equal, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1); \
extern template std::pair<BAT<v2_resoid_t, V2SELECTTYPE>*, std::vector<bool>*> selectAN<std::less, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, TYPE && th1);

            V2_SELECT(v2_restiny_t, v2_restiny_t, restiny_t)
            V2_SELECT(v2_resshort_t, v2_resshort_t, resshort_t)
            V2_SELECT(v2_resint_t, v2_resint_t, resint_t)
            V2_SELECT(v2_resbigint_t, v2_resbigint_t, resbigint_t)
            V2_SELECT(v2_resstr_t, v2_resstr_t, str_t)

#undef V2_SELECT

        }
    }
}

#endif /* OPERATORSAN_TCC */
