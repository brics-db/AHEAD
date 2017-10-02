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
 * File:   selectAN_restiny.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 02-10-2017 13:17
 */

#include <column_operators/ANbase.hpp>
#include <column_operators/functors.hpp>
#include "selectAN.hpp"

namespace ahead {
    namespace bat {
        namespace ops {

            namespace scalar {
                SELECT_AN(v2_void_t, v2_restiny_t)
                SELECT_AN_REENC(v2_void_t, v2_restiny_t)
            }

            namespace simd {
                namespace sse {
                    SELECT_AN(v2_void_t, v2_restiny_t)
                    SELECT_AN_REENC(v2_void_t, v2_restiny_t)
                }
            }

        }
    }
}
