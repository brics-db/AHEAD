// Copyright (c) 2016 Till Kolditz
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

/* 
 * File:   resilience.hpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 29. Juli 2016, 12:08
 */

#ifndef RESILIENCE_HPP
#define RESILIENCE_HPP

#include <ColumnStore.h>
#include <column_storage/ColumnBat.h>

typedef uint64_t resoid_t;

#define RESOID_INVALID (static_cast<resoid_t>(-1))

struct v2_restiny_t {
    typedef restiny_t type_t;
    typedef v2_tinyint_t unenc_v2_t;

    static const char* BASENAME;
    static const restiny_t A;
    static const restiny_t A_INV;
    static const restiny_t A_UNENC_MIN;
    static const restiny_t A_UNENC_MAX;
    static const restiny_t A_UNENC_MAX_U;
};

struct v2_resshort_t {
    typedef resshort_t type_t;
    typedef v2_shortint_t unenc_v2_t;

    static const char* BASENAME;
    static const resshort_t A;
    static const resshort_t A_INV;
    static const resshort_t A_UNENC_MIN;
    static const resshort_t A_UNENC_MAX;
    static const resshort_t A_UNENC_MAX_U;
};

struct v2_resint_t {
    typedef resint_t type_t;
    typedef v2_int_t unenc_v2_t;

    static const char* BASENAME;
    static const resint_t A;
    static const resint_t A_INV;
    static const resint_t A_UNENC_MIN;
    static const resint_t A_UNENC_MAX;
    static const resint_t A_UNENC_MAX_U;
};

struct v2_resoid_t {
    typedef resoid_t type_t;
    typedef v2_oid_t unenc_v2_t;

    static const char* BASENAME;
    static const resoid_t A;
    static const resoid_t A_INV;
    static const resoid_t A_UNENC_MIN;
    static const resoid_t A_UNENC_MAX;
    static const resoid_t A_UNENC_MAX_U;
};

template<typename Tail>
class ColumnBatIterator<v2_resoid_t, Tail> : public ColumnBatIteratorBase<v2_resoid_t, Tail> {
public:
    using ColumnBatIteratorBase<v2_resoid_t, Tail>::ColumnBatIteratorBase;

    virtual ~ColumnBatIterator() {
    }

    virtual pair<resoid_t, Tail>&& get(unsigned index) override {
        this->bu = this->ta->get(this->mColumnId, index);
        pair<resoid_t, Tail> p = make_pair(std::move(*reinterpret_cast<resoid_t*> (&this->bu->head)), std::move(static_cast<const char*> (this->bu->tail)));
        delete this->bu;
        this->bu = this->ta->next(this->mColumnId);
        return p;
    }

    virtual pair<resoid_t, Tail>&& next() override {
        pair<resoid_t, Tail> p = make_pair(std::move(*reinterpret_cast<resoid_t*> (&this->bu->head)), std::move(static_cast<const char*> (this->bu->tail)));
        delete this->bu;
        this->bu = this->ta->next(this->mColumnId);
        return p;
    }
};

template<typename Tail>
class ColumnBat<v2_resoid_t, Tail> : public Bat<v2_resoid_t, Tail> {
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
    virtual BatIterator<v2_resoid_t, Tail> * begin() override {
        return new ColumnBatIterator<v2_resoid_t, Tail>(mColumnId);
    }

    /** append an item */
    virtual void append(pair<resoid_t, Tail>& p) override {
    }

    virtual void append(pair<resoid_t, Tail>&& p) override {
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

#endif /* RESILIENCE_HPP */
