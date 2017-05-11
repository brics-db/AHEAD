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
 * File:   selectAN.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 10-04-2017 22:10
 */

#include <column_operators/AN/selectAN.tcc>

namespace ahead {
    namespace bat {
        namespace ops {

#define V2_SELECT2_AN_SUB(SELECT, V2TYPE) \
template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> scalar::selectAN<std::greater, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t && th1, typename V2TYPE::type_t && th2); \
template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> scalar::selectAN<std::greater_equal, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg,typename V2TYPE::type_t && th1,typename V2TYPE::type_t && th2); \
template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> scalar::selectAN<std::equal_to, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg,typename V2TYPE::type_t && th1,typename V2TYPE::type_t && th2); \
template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> scalar::selectAN<std::less_equal, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg,typename V2TYPE::type_t && th1,typename V2TYPE::type_t && th2); \
template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> scalar::selectAN<std::less, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg,typename V2TYPE::type_t && th1,typename V2TYPE::type_t && th2); \
template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> scalar::selectAN<std::not_equal_to, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg,typename V2TYPE::type_t && th1,typename V2TYPE::type_t && th2); \
template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> sse::selectAN<std::greater, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t && th1, typename V2TYPE::type_t && th2); \
template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> sse::selectAN<std::greater_equal, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg,typename V2TYPE::type_t && th1,typename V2TYPE::type_t && th2); \
template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> sse::selectAN<std::equal_to, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg,typename V2TYPE::type_t && th1,typename V2TYPE::type_t && th2); \
template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> sse::selectAN<std::less_equal, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg,typename V2TYPE::type_t && th1,typename V2TYPE::type_t && th2); \
template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> sse::selectAN<std::less, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg,typename V2TYPE::type_t && th1,typename V2TYPE::type_t && th2); \
template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> sse::selectAN<std::not_equal_to, SELECT, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg,typename V2TYPE::type_t && th1,typename V2TYPE::type_t && th2);

#define V2_SELECT2_AN(V2TYPE) \
V2_SELECT2_AN_SUB(std::greater, V2TYPE) \
V2_SELECT2_AN_SUB(std::greater_equal, V2TYPE) \
V2_SELECT2_AN_SUB(std::equal_to, V2TYPE) \
V2_SELECT2_AN_SUB(std::less_equal, V2TYPE) \
V2_SELECT2_AN_SUB(std::less, V2TYPE) \
V2_SELECT2_AN_SUB(std::not_equal_to, V2TYPE)

#define V2_SELECT_AN(V2TYPE) \
template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> scalar::selectAN<std::greater, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t && th1); \
template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> scalar::selectAN<std::greater_equal, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t && th1); \
template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> scalar::selectAN<std::equal_to, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t && th1); \
template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> scalar::selectAN<std::less_equal, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t && th1); \
template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> scalar::selectAN<std::less, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t && th1); \
template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> scalar::selectAN<std::not_equal_to, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t && th1); \
template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> sse::selectAN<std::greater, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t && th1); \
template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> sse::selectAN<std::greater_equal, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t && th1); \
template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> sse::selectAN<std::equal_to, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t && th1); \
template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> sse::selectAN<std::less_equal, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t && th1); \
template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> sse::selectAN<std::less, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t && th1); \
template std::pair<BAT<v2_resoid_t, typename V2TYPE::v2_select_t>*, AN_indicator_vector*> sse::selectAN<std::not_equal_to, v2_void_t, V2TYPE> (BAT<v2_void_t, V2TYPE>* arg, typename V2TYPE::type_t && th1); \
V2_SELECT2_AN(V2TYPE)

            V2_SELECT_AN(v2_restiny_t)
            V2_SELECT_AN(v2_resshort_t)
            V2_SELECT_AN(v2_resint_t)
            // V2_SELECT_AN(v2_resbigint_t)
            // V2_SELECT_AN(v2_resstr_t)

#undef V2_SELECT_AN
#undef V2_SELECT2_AN
#undef V2_SELECT2_AN_SUB

        }
    }
}
