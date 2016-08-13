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

#include "column_storage/BatIterator.h"
#include "column_storage/TransactionManager.h"

template<typename Head, typename Tail>
class ColumnBatIteratorBase : public BatIterator<Head, Tail> {
protected:
    TransactionManager::Transaction* ta;
    TransactionManager::BinaryUnit* bu;
    unsigned mColumnId;
    size_t Csize;
    size_t Cconsumption;
public:

    /** default constructor */
    ColumnBatIteratorBase(unsigned columnId) : mColumnId(columnId) {
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
    virtual pair<Head, Tail> next() override = 0;

    /** @return true if a next item is available - otherwise false */
    virtual bool hasNext() override {
        return (bu != NULL);
    }

    virtual pair<Head, Tail> get(unsigned index) override = 0;

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

    virtual pair<Head, Tail> get(unsigned index) override {
        this->bu = this->ta->get(this->mColumnId, index);
        pair<Head, Tail> p = make_pair(std::move(*reinterpret_cast<Head*> (&this->bu->head)), std::move(*static_cast<Tail*> (this->bu->tail)));
        delete this->bu;
        this->bu = this->ta->next(this->mColumnId);
        return p;
    }

    virtual pair<Head, Tail> next() override {
        pair<Head, Tail> p = make_pair(std::move(*reinterpret_cast<Head*> (&this->bu->head)), std::move(*static_cast<Tail*> (this->bu->tail)));
        delete this->bu;
        this->bu = this->ta->next(this->mColumnId);
        return p;
    }
};

template<typename Head>
class ColumnBatIterator<Head, const char*> : public ColumnBatIteratorBase<Head, const char*> {
public:
    using ColumnBatIteratorBase<Head, const char*>::ColumnBatIteratorBase;

    virtual ~ColumnBatIterator() {
    }

    virtual pair<Head, const char*> get(unsigned index) override {
        this->bu = this->ta->get(this->mColumnId, index);
        pair<Head, const char*> p = make_pair(std::move(*reinterpret_cast<Head*> (&this->bu->head)), std::move(static_cast<const char*> (this->bu->tail)));
        delete this->bu;
        this->bu = this->ta->next(this->mColumnId);
        return p;
    }

    virtual pair<Head, const char*> next() override {
        pair<Head, const char*> p = make_pair(std::move(*reinterpret_cast<Head*> (&this->bu->head)), std::move(static_cast<const char*> (this->bu->tail)));
        delete this->bu;
        this->bu = this->ta->next(this->mColumnId);
        return p;
    }
};

#endif

