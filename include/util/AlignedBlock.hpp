// Copyright 2016 Till Kolditz, Stefan de Bruijn
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

#ifndef ALIGNEDBLOCK_HPP
#define ALIGNEDBLOCK_HPP

#include <cstring>
#include <cstdint>
#include <memory>

namespace ahead {

    struct AlignedBlock {

        const size_t nBytes;
        const size_t alignment;

    private:
        std::shared_ptr<char> baseptr;
        void* data;

    public:

        AlignedBlock()
                : nBytes(0),
                  alignment(0),
                  baseptr(nullptr),
                  data(nullptr) {
        }

        AlignedBlock(
                size_t nBytes,
                size_t alignment)
                : nBytes(nBytes),
                  alignment(alignment),
                  baseptr(new char[nBytes + alignment]),
                  data(nullptr) {
            size_t tmp = reinterpret_cast<size_t>(baseptr.get());
            data = baseptr.get() + (alignment - (tmp & (alignment - 1)));
        }

        AlignedBlock(
                AlignedBlock & other)
                : nBytes(other.nBytes),
                  alignment(other.alignment),
                  baseptr(other.baseptr),
                  data(other.data) {
        }

        AlignedBlock & operator=(
                AlignedBlock & other) {
            this->~AlignedBlock();
            new (this) AlignedBlock(other);
            return *this;
        }

        template<typename T = void>
        T*
        begin() {
            return static_cast<T*>(data);
        }

        template<typename T = void>
        constexpr T*
        end() const {
            return reinterpret_cast<T*>(static_cast<char*>(data) + nBytes);
        }

        virtual ~AlignedBlock() {
            data = nullptr;
            // baseptr is now a std::shared_ptr and auto-deallocated, which deletes its contents as well
        }
    };

}

#endif /* ALIGNEDBLOCK_HPP */
