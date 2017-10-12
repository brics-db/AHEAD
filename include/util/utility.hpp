// Copyright 2017 Till Kolditz
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
 * utility.hpp
 *
 *  Created on: 12.10.2017
 *      Author: Till Kolditz - Till.Kolditz@gmail.com
 */

#pragma once

#include <cstdlib>
#include <vector>

template<typename T, template<typename > class vec_allocator>
void overwrite_vector_size(
        std::vector<T, vec_allocator> vec,
        size_t newSize) {
#ifdef __GNUC__
    typedef std::_Vector_base<T, vec_allocator> vec_base_t;
    vec_base_t & base = (vec_base_t &) *vec.get();
    base._M_impl._M_finish = base._M_impl._M_start + newSize;
#else
#error "overwrite_size is only supported for GCC compilers, i.e. when macro __GNUC__ is defined"
#endif
}
