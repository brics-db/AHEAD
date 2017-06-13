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
 * File:   miscellaneous.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 10-04-2017 22:03
 */

#include "miscellaneous.tcc"

namespace ahead {
    namespace bat {
        namespace ops {

            template TempBAT<v2_void_t, v2_oid_t>* copy(
                    BAT<v2_void_t, v2_oid_t>* arg);
            template TempBAT<v2_void_t, v2_id_t>* copy(
                    BAT<v2_void_t, v2_id_t>* arg);
            template TempBAT<v2_void_t, v2_size_t>* copy(
                    BAT<v2_void_t, v2_size_t>* arg);
            template TempBAT<v2_void_t, v2_tinyint_t>* copy(
                    BAT<v2_void_t, v2_tinyint_t>* arg);
            template TempBAT<v2_void_t, v2_shortint_t>* copy(
                    BAT<v2_void_t, v2_shortint_t>* arg);
            template TempBAT<v2_void_t, v2_int_t>* copy(
                    BAT<v2_void_t, v2_int_t>* arg);
            template TempBAT<v2_void_t, v2_bigint_t>* copy(
                    BAT<v2_void_t, v2_bigint_t>* arg);
            template TempBAT<v2_void_t, v2_str_t>* copy(
                    BAT<v2_void_t, v2_str_t>* arg);
            template TempBAT<v2_void_t, v2_restiny_t>* copy(
                    BAT<v2_void_t, v2_restiny_t>* arg);
            template TempBAT<v2_void_t, v2_resshort_t>* copy(
                    BAT<v2_void_t, v2_resshort_t>* arg);
            template TempBAT<v2_void_t, v2_resint_t>* copy(
                    BAT<v2_void_t, v2_resint_t>* arg);
            template TempBAT<v2_void_t, v2_resbigint_t>* copy(
                    BAT<v2_void_t, v2_resbigint_t>* arg);
            template TempBAT<v2_void_t, v2_resstr_t>* copy(
                    BAT<v2_void_t, v2_resstr_t>* arg);

        }
    }
}
