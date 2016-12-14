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
#include <column_storage/ColumnMetaData.hpp>
#include <column_storage/ColumnManager.h>
#include <column_storage/TransactionManager.h>
#include <meta_repository/MetaRepositoryManager.h>
#include <column_storage/ColumnBatIterator.h>

template<typename Tail>
class ColumnBAT : public BAT<v2_void_t, Tail> {

    typedef v2_void_t Head;
    typedef Head v2_head_t;
    typedef Tail v2_tail_t;
    typedef typename Head::type_t head_t;
    typedef typename Tail::type_t tail_t;
    typedef ColumnDescriptor<v2_void_t, void> coldesc_head_t;
    typedef ColumnDescriptor<Tail, vector<tail_t>> coldesc_tail_t;

    id_t mColumnId;

public:

    coldesc_head_t head;
    coldesc_tail_t tail;

    ColumnBAT (id_t columnId) : mColumnId (columnId), head (), tail () {
        tail.metaData = ColumnManager::getInstance()->getColumnMetaData(mColumnId);
    }

    ColumnBAT (const char *table_name, const char *attribute) : mColumnId (0), head (), tail () {
        mColumnId = MetaRepositoryManager::getInstance()->getBatIdOfAttribute(table_name, attribute);
        tail.metaData = ColumnManager::getInstance()->getColumnMetaData(mColumnId);
    }

    virtual
    ~ColumnBAT () {
    }

    /** returns an iterator pointing at the start of the column */
    virtual BATIterator<Head, Tail> *
    begin () override {
        return new ColumnBatIterator<Head, Tail>(mColumnId);
    }

    /** append an item */
    virtual void
    append (__attribute__ ((unused)) pair<head_t, tail_t>& p) override {
    }

    virtual void
    append (__attribute__ ((unused)) pair<head_t, tail_t>&& p) override {
    }

    virtual void
    append (__attribute__ ((unused)) tail_t& t) override {
    }

    virtual void
    append (__attribute__ ((unused)) tail_t&& t) override {
    }

    virtual BAT<Tail, Head>*
    reverse () override {
        return nullptr;
    }

    virtual BAT<Head, Head>*
    mirror_head () override {
        return nullptr;
    }

    virtual BAT<Tail, Tail>*
    mirror_tail () override {
        return nullptr;
    }

    virtual unsigned
    size () override {
        auto iter = begin();
        unsigned size = iter->size();
        delete iter;
        return size;
    }

    virtual size_t
    consumption () override {
        auto iter = begin();
        unsigned size = iter->consumption();
        delete iter;
        return size;
    }
};

#endif
