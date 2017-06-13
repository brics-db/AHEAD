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
 * File:   encdecAN.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 31-05-2017 10:06
 */

#include <column_operators/ANbase.hpp>
#include "encdecAN.tcc"

namespace ahead {
    namespace bat {
        namespace ops {

#define CHECK_AND_DECODE_AN_SUB(V2Head, V2Tail)                                                                              \
template std::tuple<BAT<typename V2Head::v2_unenc_t, typename V2Tail::v2_unenc_t>*, std::vector<bool>*, std::vector<bool>*>  \
checkAndDecodeAN(BAT<V2Head, V2Tail>* arg)

#define CHECK_AND_DECODE_AN(V2Tail) \
CHECK_AND_DECODE_AN_SUB(v2_void_t, V2Tail); \
CHECK_AND_DECODE_AN_SUB(v2_oid_t, V2Tail);

            namespace scalar {
                CHECK_AND_DECODE_AN(v2_restiny_t)
                CHECK_AND_DECODE_AN(v2_resshort_t)
                CHECK_AND_DECODE_AN(v2_resint_t)
                CHECK_AND_DECODE_AN(v2_resoid_t)
                CHECK_AND_DECODE_AN(v2_resbigint_t)
            }

            namespace sse {
                CHECK_AND_DECODE_AN(v2_restiny_t)
                CHECK_AND_DECODE_AN(v2_resshort_t)
                CHECK_AND_DECODE_AN(v2_resint_t)
                CHECK_AND_DECODE_AN(v2_resoid_t)
                CHECK_AND_DECODE_AN(v2_resbigint_t)
            }

        }
    }
}
