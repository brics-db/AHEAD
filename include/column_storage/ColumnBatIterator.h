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
    TransactionManager::BinaryUnit* bu;
    id_t mColumnId;
    size_t Csize;
    size_t Cconsumption;

public:

    /** default constructor */
    ColumnBatIteratorBase(id_t columnId) : mColumnId(columnId) {
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
        bu = ta->next(mColumnId);
    }

    virtual ~ColumnBatIteratorBase() {
        if (ta) {
            TransactionManager* tm = TransactionManager::getInstance();
            tm->endTransaction(ta);
            ta = nullptr;
        }
        if (bu) {
            delete bu;
            bu = nullptr;
        }
    }

    /** iterator next */
    virtual pair<oid_t, tail_t>&& next() override = 0;

    /** @return true if a next item is available - otherwise false */
    virtual bool hasNext() override {
        return (bu != NULL);
    }

    virtual pair<oid_t, tail_t>&& get(size_t index) override = 0;

    virtual size_t size() override {
        return Csize;
    }

    virtual size_t consumption() override {
        return Cconsumption;
    }
};

template<typename Tail>
class ColumnBatIterator<v2_oid_t, Tail> : public ColumnBatIteratorBase<v2_oid_t, Tail> {
    typedef typename Tail::type_t tail_t;

public:
    using ColumnBatIteratorBase<v2_oid_t, Tail>::ColumnBatIteratorBase;

    virtual ~ColumnBatIterator() {
    }

    virtual pair<oid_t, tail_t>&& get(size_t index) override {
        this->bu = this->ta->get(this->mColumnId, index);
        auto p = make_pair(move(*reinterpret_cast<oid_t*> (&this->bu->head)), move(*static_cast<tail_t*> (this->bu->tail)));
        delete this->bu;
        this->bu = this->ta->next(this->mColumnId);
        return move(p);
    }

    virtual pair<v2_oid_t, Tail>&& next() override {
        auto p = make_pair(move(*reinterpret_cast<oid_t*> (&this->bu->head)), move(*static_cast<tail_t*> (this->bu->tail)));
        delete this->bu;
        this->bu = this->ta->next(this->mColumnId);
        return move(p);
    }
};

template<>
class ColumnBatIterator<v2_oid_t, v2_cstr_t> : public ColumnBatIteratorBase<v2_oid_t, v2_cstr_t> {
public:
    using ColumnBatIteratorBase<v2_oid_t, v2_cstr_t>::ColumnBatIteratorBase;

    virtual ~ColumnBatIterator() {
    }

    virtual pair<oid_t, cstr_t>&& get(size_t index) override {
        this->bu = this->ta->get(this->mColumnId, index);
        auto p = make_pair(move(*reinterpret_cast<oid_t*> (&this->bu->head)), move(static_cast<cstr_t> (this->bu->tail)));
        delete this->bu;
        this->bu = this->ta->next(this->mColumnId);
        return move(p);
    }

    virtual pair<oid_t, cstr_t>&& next() override {
        auto p = make_pair(move(*reinterpret_cast<oid_t*> (&this->bu->head)), move(static_cast<cstr_t> (this->bu->tail)));
        delete this->bu;
        this->bu = this->ta->next(this->mColumnId);
        return move(p);
    }
};

#endif

