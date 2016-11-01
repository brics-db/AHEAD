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
    ColumnBatIteratorBase(id_t columnId) : ta(nullptr), bu(), buNext(), mColumnId(columnId), Csize(0), Cconsumption(0), mPosition(-1) {
        TransactionManager* tm = TransactionManager::getInstance();
        if (tm == nullptr) {
            cerr << "TA manager is not available!" << endl;
            abort();
        }
        ta = tm->beginTransaction(false);
        if (ta == nullptr) {
            cout << "Column is not available!" << endl;
        }
        tie(Csize, Cconsumption) = ta->open(columnId);
        buNext = ta->next(mColumnId);
        next(); // init to first 
    }

    ColumnBatIteratorBase(const ColumnBatIteratorBase<Head, Tail> &iter) : ta(iter.ta), bu(iter.bu), buNext(iter.buNext), mColumnId(iter.mColumnId), Csize(iter.Csize), Cconsumption(iter.Cconsumption), mPosition(iter.mPosition) {
    }

    virtual ~ColumnBatIteratorBase() {
        if (ta) {
            TransactionManager* tm = TransactionManager::getInstance();
            tm->endTransaction(ta);
            ta = nullptr;
        }
    }

    ColumnBatIteratorBase& operator=(const ColumnBatIteratorBase &copy) {
        new (this) ColumnBatIteratorBase(copy);
        return *this;
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
        return mPosition < static_cast<ssize_t> (Csize);
    }

    virtual oid_t head() override {
        return oid_t(this->mPosition);
    }

    virtual tail_t tail() override {
        return *static_cast<tail_t*> (bu.tail);
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
    typedef ColumnBatIterator<Head, Tail> self_t;

    using ColumnBatIteratorBase<Head, Tail>::ColumnBatIteratorBase;

    virtual ~ColumnBatIterator() {
    }
};

template<>
class ColumnBatIterator<v2_void_t, v2_cstr_t> : public ColumnBatIteratorBase<v2_void_t, v2_cstr_t> {
public:
    typedef ColumnBatIterator<v2_void_t, v2_cstr_t> self_t;

    using ColumnBatIteratorBase<v2_void_t, v2_cstr_t>::ColumnBatIteratorBase;

    virtual ~ColumnBatIterator() {
    }

    virtual cstr_t tail() override {
        return cstr_t(this->bu.tail);
    }
};

#endif
