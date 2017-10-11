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
 * AlignedAllocator.hpp
 *
 *  Created on: 11.10.2017
 *      Author: Till Kolditz - Till.Kolditz@gmail.com
 *     Credits: https://stackoverflow.com/questions/12942548/making-stdvector-allocate-aligned-memory
 */

#pragma once

#include <cstdlib>
#include <type_traits>
#include <memory>

namespace ahead {

    enum alignment_type_t
        : size_t
        {
            Scalar = sizeof(void*),
        SSE = 16,
        AVX = 32,
    };

    namespace detail {

        void* allocate_aligned_memory(
                size_t align,
                size_t size);
        void deallocate_aligned_memory(
                void* ptr) noexcept;

    }

    template<typename T, alignment_type_t Align = alignment_type_t::AVX>
    class AlignedAllocator;

    template<alignment_type_t Align>
    class AlignedAllocator<void, Align> {
    public:
        typedef void* pointer;
        typedef const void* const_pointer;
        typedef void value_type;

        template<class U> struct rebind {
            typedef AlignedAllocator<U, Align> other;
        };
    };

    template<typename T, alignment_type_t Align>
    class AlignedAllocator {
    public:
        typedef T value_type;
        typedef T* pointer;
        typedef const T* const_pointer;
        typedef T& reference;
        typedef const T& const_reference;
        typedef size_t size_type;
        typedef ptrdiff_t difference_type;

        typedef std::true_type propagate_on_container_move_assignment;

        template<class U>
        struct rebind {
            typedef AlignedAllocator<U, Align> other;
        };

    public:
        AlignedAllocator() noexcept
        {
        }

        template<class U>
        AlignedAllocator(
                const AlignedAllocator<U, Align>&) noexcept
                {
        }

        size_type max_size() const noexcept
        {
            return (size_type(~0) - size_type(Align)) / sizeof(T);
        }

        pointer address(
                reference x) const noexcept
                {
            return std::addressof(x);
        }

        const_pointer address(
                const_reference x) const noexcept
                {
            return std::addressof(x);
        }

        pointer allocate(
                size_type n,
                typename AlignedAllocator<void, Align>::const_pointer = 0) {
            const size_type alignment = static_cast<size_type>(Align);
            void* ptr = detail::allocate_aligned_memory(alignment, n * sizeof(T));
            if (ptr == nullptr) {
                throw std::bad_alloc();
            }

            return reinterpret_cast<pointer>(ptr);
        }

        void deallocate(
                pointer p,
                size_type) noexcept
                {
            detail::deallocate_aligned_memory(p);
        }

        template<class U, class ...Args>
        void construct(
                U* p,
                Args&&... args) {
            ::new (reinterpret_cast<void*>(p)) U(std::forward<Args>(args)...);
        }

        void destroy(
                pointer p) {
            p->~T();
        }
    };

    template<typename T, alignment_type_t Align>
    class AlignedAllocator<const T, Align> {
    public:
        typedef T value_type;
        typedef const T* pointer;
        typedef const T* const_pointer;
        typedef const T& reference;
        typedef const T& const_reference;
        typedef size_t size_type;
        typedef ptrdiff_t difference_type;

        typedef std::true_type propagate_on_container_move_assignment;

        template<class U>
        struct rebind {
            typedef AlignedAllocator<U, Align> other;
        };

    public:
        AlignedAllocator() noexcept
        {
        }

        template<class U>
        AlignedAllocator(
                const AlignedAllocator<U, Align>&) noexcept
                {
        }

        size_type max_size() const noexcept
        {
            return (size_type(~0) - size_type(Align)) / sizeof(T);
        }

        const_pointer address(
                const_reference x) const noexcept
                {
            return std::addressof(x);
        }

        pointer allocate(
                size_type n,
                typename AlignedAllocator<void, Align>::const_pointer = 0) {
            const size_type alignment = static_cast<size_type>(Align);
            void* ptr = detail::allocate_aligned_memory(alignment, n * sizeof(T));
            if (ptr == nullptr) {
                throw std::bad_alloc();
            }

            return reinterpret_cast<pointer>(ptr);
        }

        void deallocate(
                pointer p,
                size_type) noexcept
                {
            detail::deallocate_aligned_memory(p);
        }

        template<class U, class ...Args>
        void construct(
                U* p,
                Args&&... args) {
            ::new (reinterpret_cast<void*>(p)) U(std::forward<Args>(args)...);
        }

        void destroy(
                pointer p) {
            p->~T();
        }
    };

    template<typename T, alignment_type_t TAlign, typename U, alignment_type_t UAlign>
    inline
    bool operator==(
            const AlignedAllocator<T, TAlign>&,
            const AlignedAllocator<U, UAlign>&) noexcept
            {
        return TAlign == UAlign;
    }

    template<typename T, alignment_type_t TAlign, typename U, alignment_type_t UAlign>
    inline
    bool operator!=(
            const AlignedAllocator<T, TAlign>&,
            const AlignedAllocator<U, UAlign>&) noexcept
            {
        return TAlign != UAlign;
    }

}
