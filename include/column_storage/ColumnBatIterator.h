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

template<class Head, class Tail>
class ColumnBatIterator : public BatIterator<Head, Tail> {
private:
    TransactionManager::Transaction* ta;
    TransactionManager::BinaryUnit* bu;
    unsigned mColumnId;
    unsigned Csize;
    bool mNext;
public:

    /** default constructor */
    ColumnBatIterator(unsigned columnId) : mColumnId(columnId) {
        mNext = false;
        TransactionManager* tm = TransactionManager::getInstance();
        if (tm == NULL) {
            cerr << "TA manager is not available!" << endl;
            abort();
        }
        ta = tm->beginTransaction(false);
        Csize = ta->open(columnId);
        if (ta == NULL) {
            cout << "Column is not available!" << endl;
        }
        bu = ta->next(mColumnId);
    };

    /** iterator next */
    virtual pair<Head, Tail> next() {
        pair<Head, Tail> p;
        memcpy(&p.first, bu->head, sizeof (Head));
        memcpy(&p.second, bu->tail, sizeof (Tail));
        delete bu->head;
        delete bu;
        bu = ta->next(mColumnId);
        return p;
    };

    /** @return true if a next item is available - otherwise false */
    virtual bool hasNext() {
        return (bu != NULL);
    };

    virtual pair<Head, Tail> get(unsigned index) {
        bu = ta->get(mColumnId, index);
        pair<Head, Tail> p;
        memcpy(&p.first, bu->head, sizeof (Head));
        memcpy(&p.second, bu->tail, sizeof (Tail));
        bu = ta->next(mColumnId);
        return p;
    };

    virtual unsigned size() {
        return Csize;
    };
};

#endif

