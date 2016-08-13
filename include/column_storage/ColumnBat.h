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

#include <column_storage/Bat.h>
#include <column_storage/ColumnManager.h>
#include <column_storage/TransactionManager.h>
#include <meta_repository/MetaRepositoryManager.h>
#include <column_storage/ColumnBatIterator.h>

using namespace std;

template<class Head, class Tail>
class ColumnBat : public Bat<Head, Tail> {
    id_t mColumnId;

public:

    ColumnBat(id_t columnId) : mColumnId(columnId) {
    }

    ColumnBat(const char *table_name, const char *attribute) {
        MetaRepositoryManager *mrm = MetaRepositoryManager::getInstance();
        mColumnId = mrm->getBatIdOfAttribute(table_name, attribute);
    }

    virtual ~ColumnBat() {
    }

    /** returns an iterator pointing at the start of the column */
    virtual BatIterator<Head, Tail> * begin() override {
        return new ColumnBatIterator<Head, Tail>(mColumnId);
    }

    /** append an item */
    virtual void append(pair<Head, Tail>& p) override {
    }

    virtual void append(pair<Head, Tail>&& p) override {
    }

    virtual unsigned size() override {
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
};

typedef ColumnBat<oid_t, oid_t> oid_col_t;
typedef ColumnBat<oid_t, tinyint_t> tinyint_col_t;
typedef ColumnBat<oid_t, shortint_t> shortint_col_t;
typedef ColumnBat<oid_t, int_t> int_col_t;
typedef ColumnBat<oid_t, largeint_t> largeint_col_t;
typedef ColumnBat<oid_t, char_t> char_col_t;
typedef ColumnBat<oid_t, str_t> str_col_t;
typedef ColumnBat<oid_t, fixed_t> fixed_col_t;

typedef Bat<oid_t, oid_t> oid_bat_t;
typedef Bat<oid_t, tinyint_t> tinyint_bat_t;
typedef Bat<oid_t, shortint_t> shortint_bat_t;
typedef Bat<oid_t, int_t> int_bat_t;
typedef Bat<oid_t, largeint_t> largeint_bat_t;
typedef Bat<oid_t, char_t> char_bat_t;
typedef Bat<oid_t, str_t> str_bat_t;
typedef Bat<oid_t, fixed_t> fixed_bat_t;

#endif
