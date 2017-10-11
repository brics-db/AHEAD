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
 * AlignedAllocator.cpp
 *
 *  Created on: 11.10.2017
 *      Author: Till Kolditz - Till.Kolditz@gmail.com
 *     Credits: https://stackoverflow.com/questions/12942548/making-stdvector-allocate-aligned-memory
 */

#include <assert.h>

#include <util/AlignedAllocator.hpp>
#include <util/memory.hpp>

namespace ahead {

    void*
    detail::allocate_aligned_memory(
            size_t align,
            size_t size) {
        assert(align >= sizeof(void*));
        assert(is_power_of_two(align));

        if (size == 0) {
            return nullptr;
        }

        void* ptr = aligned_alloc(align, size);

        return ptr;
    }

    void detail::deallocate_aligned_memory(
            void *ptr) noexcept
            {
        return free(ptr);
    }

}
