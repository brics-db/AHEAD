// Copyright (c) 2010 Benjamin Schlegel
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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

/***
 * @author Benjamin Schlegel
 * @author Till Kolditz
 */
#ifndef LIB_COLUMNBAT_HPP
#define LIB_COLUMNBAT_HPP

#include <vector>
#include <iostream>
#include <cstdlib>

#include <ColumnStore.h>
#include <column_storage/Bat.h>
#include <column_storage/ColumnManager.h>
#include <column_storage/ColumnBatIterator.h>
#include "../meta_repository/MetaRepositoryManager.h"

namespace ahead {

    template<typename Tail>
    class ColumnBAT :
            public BAT<v2_void_t, Tail> {

        typedef v2_void_t Head;
        using v2_head_t = typename BAT<Head, Tail>::v2_head_t;
        using v2_tail_t = typename BAT<Head, Tail>::v2_tail_t;
        using head_t = typename BAT<Head, Tail>::head_t;
        using tail_t = typename BAT<Head, Tail>::tail_t;
        using coldesc_head_t = typename BAT<Head, Tail>::coldesc_head_t;
        using coldesc_tail_t = typename BAT<Head, Tail>::coldesc_tail_t;

        id_t mColumnId;

    public:

        ColumnBAT(
                id_t columnId)
                : mColumnId(columnId) {
            this->tail.metaData = ColumnManager::getInstance()->getColumnMetaData(mColumnId);
        }

        ColumnBAT(
                cstr_t tableName,
                cstr_t attributeName)
                : ColumnBAT(MetaRepositoryManager::getInstance()->getBatIdOfAttribute(tableName, attributeName)) {
            MetaRepositoryManager::getInstance()->template testDataTypeForAttribute<Tail>(tableName, attributeName);
        }

        virtual ~ColumnBAT() {
        }

        /** returns an iterator pointing at the start of the column */
        virtual BATIterator<Head, Tail> *
        begin() override {
            return new ColumnBatIterator<Head, Tail>(mColumnId);
        }

        virtual BAT<Tail, Head> *
        reverse() override {
            return nullptr;
        }

        virtual BAT<Head, Head> *
        mirror_head() override {
            return nullptr;
        }

        virtual BAT<Tail, Tail> *
        mirror_tail() override {
            return nullptr;
        }

        virtual BAT<v2_void_t, Tail> *
        clear_head() override {
            return this;
        }

        virtual oid_t size() override {
            auto iter = begin();
            unsigned size = iter->size();
            delete iter;
            return size;
        }

        virtual size_t consumption() override {
            auto iter = begin();
            unsigned size = iter->consumption();
            delete iter;
            return size;
        }

        virtual size_t consumptionProjected() override {
            size_t szTail = BITS_SIZEOF(typename TypeMap<Tail>::v2_base_t::type_t);
            if (std::is_base_of<v2_anencoded_t, Tail>::value) {
                szTail += BITS_SIZEOF(this->tail.metaData.AN_A) - BITS_CLZ(this->tail.metaData.AN_A);
            }
            return BITS_TO_BYTES(size() * szTail);
        }
    };

}

#endif /* LIB_COLUMNBAT_HPP */
