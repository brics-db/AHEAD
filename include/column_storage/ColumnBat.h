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
#ifndef COLUMNBAT_H
#define COLUMNBAT_H

#include <vector>
#include <iostream>
#include <cstdlib>

#include "column_storage/Bat.h"
#include "column_storage/ColumnManager.h"
#include "column_storage/TransactionManager.h"
#include "meta_repository/MetaRepositoryManager.h"
#include "column_storage/ColumnBatIterator.h"

using namespace std;

template<class Head, class Tail>
class ColumnBat : public Bat<Head, Tail> {
private:
    unsigned mColumnId;
public:

    /** default constructor */
    ColumnBat(unsigned columnId) : mColumnId(columnId) {
    };

    ColumnBat(const char *table_name, const char *attribute) {
        MetaRepositoryManager *mrm = MetaRepositoryManager::getInstance();
        mColumnId = mrm->getBatIdOfAttribute(table_name, attribute);
    };

    /** returns an iterator pointing at the start of the column */
    virtual BatIterator<Head, Tail> * begin() {
        return new ColumnBatIterator<Head, Tail>(mColumnId);
    };

    /** append an item */
    virtual void append(pair<Head, Tail> p) {
    };

    virtual int size() {
        cout << "ColoumnBat::size() called - is just a dummy for now!";
        return 0;
    }
    //	virtual std::pair<Head,Tail> pop() { std::cout<<"ColumnBat::pop() called - is just a dummy for now!"; }
};

#endif
