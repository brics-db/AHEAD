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

/***
 * @author Benjamin Schlegel
 */
#ifndef COLUMNBATITERATOR_H
#define COLUMNBATITERATOR_H

#include <column_storage/BatIterator.h>
#include <column_storage/TransactionManager.h>

template<typename Head, typename Tail>
class ColumnBatIteratorBase : public BatIterator<Head, Tail> {
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
    ColumnBatIteratorBase(id_t columnId) : bu(), mColumnId(columnId), mPosition(-1) {
        TransactionManager* tm = TransactionManager::getInstance();
        if (tm == NULL) {
            cerr << "TA manager is not available!" << endl;
            abort();
        }
        ta = tm->beginTransaction(false);
        tie(Csize, Cconsumption) = ta->open(columnId);
        if (ta == NULL) {
            cout << "Column is not available!" << endl;
        }
        buNext = ta->next(mColumnId);
    }

    virtual ~ColumnBatIteratorBase() {
        if (ta) {
            TransactionManager* tm = TransactionManager::getInstance();
            tm->endTransaction(ta);
            ta = nullptr;
        }
    }

    virtual void position(oid_t index) override {
        bu = ta->get(mColumnId, index);
        mPosition = index;
        buNext = ta->next(mColumnId);
    }

    /** iterator next */
    virtual void next() override {
        ++mPosition;
        bu = buNext; // save actual current position
        buNext = ta->next(mColumnId);
    }

    virtual ColumnBatIteratorBase<Head, Tail>& operator++() override {
        next();
        return *this;
    }

    /** @return true if a next item is available - otherwise false */
    virtual bool hasNext() override {
        return (buNext.tail != nullptr);
    }

    virtual head_t&& head() override {
        return move(*static_cast<head_t*> (bu.head));
    }

    virtual tail_t&& tail() override {
        return move(*static_cast<tail_t*> (bu.tail));
    }

    virtual size_t size() override {
        return Csize;
    }

    virtual size_t consumption() override {
        return Cconsumption;
    }
};

template<typename Head, typename Tail>
class ColumnBatIterator : public ColumnBatIteratorBase<Head, Tail> {
public:
    using ColumnBatIteratorBase<Head, Tail>::ColumnBatIteratorBase;

    virtual ~ColumnBatIterator() {
    }
};

template<typename Head>
class ColumnBatIterator<Head, v2_void_t> : public ColumnBatIteratorBase<Head, v2_void_t> {
public:
    using ColumnBatIteratorBase<Head, v2_void_t>::ColumnBatIteratorBase;

    virtual ~ColumnBatIterator() {
    }

    virtual oid_t&& tail() override {
        return move(static_cast<oid_t> (this->mPosition));
    }
};

template<typename Tail>
class ColumnBatIterator<v2_void_t, Tail> : public ColumnBatIteratorBase<v2_void_t, Tail> {
public:
    using ColumnBatIteratorBase<v2_void_t, Tail>::ColumnBatIteratorBase;

    virtual ~ColumnBatIterator() {
    }

    virtual oid_t&& head() override {
        return move(static_cast<oid_t> (this->mPosition));
    }
};

template<>
class ColumnBatIterator<v2_void_t, v2_cstr_t> : public ColumnBatIteratorBase<v2_void_t, v2_cstr_t> {
public:
    using ColumnBatIteratorBase<v2_void_t, v2_cstr_t>::ColumnBatIteratorBase;

    virtual ~ColumnBatIterator() {
    }

    virtual oid_t&& head() override {
        return move(static_cast<oid_t> (this->mPosition));
    }

    virtual cstr_t&& tail() override {
        return move(static_cast<cstr_t> (this->bu.tail));
    }
};

template<>
class ColumnBatIterator<v2_cstr_t, v2_void_t> : public ColumnBatIteratorBase<v2_cstr_t, v2_void_t> {
public:
    using ColumnBatIteratorBase<v2_cstr_t, v2_void_t>::ColumnBatIteratorBase;

    virtual ~ColumnBatIterator() {
    }

    virtual cstr_t&& head() override {
        return move(static_cast<cstr_t> (this->bu.tail));
    }

    virtual oid_t&& tail() override {
        return move(static_cast<oid_t> (this->mPosition));
    }
};

#endif
