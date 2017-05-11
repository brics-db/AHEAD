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
 * File:   ANbase.hpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 03-05-2017 12:59
 */
#ifndef COLUMN_OPERATORS_AN_ANBASE_HPP_
#define COLUMN_OPERATORS_AN_ANBASE_HPP_

#include <vector>

#include <ColumnStore.h>

namespace ahead {
    namespace bat {
        namespace ops {

            typedef std::vector<resoid_t> AN_indicator_vector;

        }
    }
}

#endif /* COLUMN_OPERATORS_AN_ANBASE_HPP_ */
