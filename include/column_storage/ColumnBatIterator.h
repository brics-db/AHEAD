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
 * 
 */
#ifndef COLUMNBATITERATOR_H
#define COLUMNBATITERATOR_H

#include <sstream>

#include <column_storage/BatIterator.h>
#include <column_storage/TransactionManager.h>

template<typename Head, typename Tail>
class ColumnBatIteratorBase : public BATIterator<Head, Tail> {

    typedef typename Head::type_t head_t;
    typedef typename Tail::type_t tail_t;

protected:
    TransactionManager::Transaction* ta;
    TransactionManager::BinaryUnit bu;
    TransactionManager::BinaryUnit buNext;
    id_t mColumnId;
    size_t Csize;
    size_t Cconsumption;
    ssize_t mPosition;

public:

    /** default constructor */
    ColumnBatIteratorBase (id_t columnId) : ta (nullptr), bu (), buNext (), mColumnId (columnId), Csize (0), Cconsumption (0), mPosition (-1) {
        TransactionManager* tm = TransactionManager::getInstance();
        if (tm == nullptr) {
            std::stringstream ss;
            ss << "TA manager is not available!" << std::endl;
            throw std::runtime_error(ss.str());
        }
        ta = tm->beginTransaction(false);
        if (ta == nullptr) {
            std::stringstream ss;
            ss << "Column is not available!" << std::endl;
            throw std::runtime_error(ss.str());
        }
        std::tie(Csize, Cconsumption) = ta->open(columnId);
        buNext = ta->next(mColumnId);
        next(); // init to first 
    }

    ColumnBatIteratorBase (const ColumnBatIteratorBase<Head, Tail> &iter) : ta (iter.ta), bu (iter.bu), buNext (iter.buNext), mColumnId (iter.mColumnId), Csize (iter.Csize), Cconsumption (iter.Cconsumption), mPosition (iter.mPosition) {
    }

    virtual
    ~ColumnBatIteratorBase () {
        if (ta) {
            TransactionManager* tm = TransactionManager::getInstance();
            tm->endTransaction(ta);
            ta = nullptr;
        }
    }

    ColumnBatIteratorBase& operator= (const ColumnBatIteratorBase &copy) {
        new (this) ColumnBatIteratorBase(copy);
        return *this;
    }

    virtual void
    position (oid_t index) override {
        bu = ta->get(mColumnId, index);
        mPosition = index;
        buNext = ta->next(mColumnId);
    }

    /** iterator next */
    virtual void
    next () override {
        ++mPosition;
        bu = buNext; // save actual current position
        buNext = ta->next(mColumnId);
    }

    virtual ColumnBatIteratorBase<Head, Tail>& operator++ () override {
        next();
        return *this;
    }

    virtual ColumnBatIteratorBase<Head, Tail>& operator+= (oid_t i) override {
        for (; i; --i) {
            next();
        }
        return *this;
    }

    /** @return true if a next item is available - otherwise false */
    virtual bool
    hasNext () override {
        return mPosition < static_cast<ssize_t>(Csize);
    }

    virtual oid_t
    head () override {
        return oid_t(this->mPosition);
    }

    virtual tail_t
    tail () override {
        return *static_cast<tail_t*>(bu.tail);
    }

    virtual size_t
    size () override {
        return Csize;
    }

    virtual size_t
    consumption () override {
        return Cconsumption;
    }
};

template<typename Head, typename Tail>
class ColumnBatIterator : public ColumnBatIteratorBase<Head, Tail> {

public:
    typedef ColumnBatIterator<Head, Tail> self_t;

    using ColumnBatIteratorBase<Head, Tail>::ColumnBatIteratorBase;

    virtual
    ~ColumnBatIterator () {
    }
};

template<typename Head>
class ColumnBatIterator<Head, v2_str_t> : public ColumnBatIteratorBase<Head, v2_str_t> {

public:
    typedef ColumnBatIterator<Head, v2_str_t> self_t;

    using ColumnBatIteratorBase<Head, v2_str_t>::ColumnBatIteratorBase;

    virtual
    ~ColumnBatIterator () {
    }

    virtual str_t
    tail () override {
        return static_cast<str_t>(this->bu.tail);
    }
};

#endif
